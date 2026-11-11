// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An upstream that is slow to send its response header: `response_header_timeout` waits for it
//! longer than `read_timeout` waits between body reads, and `upstream_response_waiting` can answer
//! the client provisionally (a header and keep-alive bytes) while it comes, also across a retry.

use async_trait::async_trait;
use pingora_core::prelude::HttpPeer;
use pingora_core::server::configuration::ServerConf;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{FailToProxy, ProxyHttp, Session};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// What the origin does with each connection.
#[derive(Clone)]
struct Origin {
    header_delay: Duration,
    body_delay: Duration,
    /// The status line and headers it answers with, without the final CRLF.
    head: &'static str,
    body: &'static str,
    /// The first connection is closed after `header_delay` with no answer at all.
    drop_first: bool,
}

async fn spawn_origin(o: Origin, connections: Arc<AtomicUsize>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let o = o.clone();
            let n = connections.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(k) => buf.extend_from_slice(&chunk[..k]),
                    }
                }
                tokio::time::sleep(o.header_delay).await;
                if o.drop_first && n == 0 {
                    return;
                }
                let head = format!("{}\r\nx-origin: yes\r\n\r\n", o.head);
                if s.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                let _ = s.flush().await;
                tokio::time::sleep(o.body_delay).await;
                let _ = s.write_all(o.body.as_bytes()).await;
                let _ = s.flush().await;
                // Keep the connection open as a real server would; the proxy closes it.
                let _ = s.read(&mut chunk).await;
            });
        }
    });
    port
}

struct WaitProxy {
    origin_port: u16,
    read_timeout: Duration,
    header_timeout: Option<Duration>,
    /// `None`: the hook is not used.
    wait: Option<Duration>,
    every: Duration,
    /// How many times the application wrote a provisional header (across every request).
    provisional_headers: Arc<AtomicUsize>,
}

#[derive(Default)]
struct Ctx {
    attempt: usize,
    /// The upstream's status, as the response filter saw it.
    status: u16,
}

#[async_trait]
impl ProxyHttp for WaitProxy {
    type CTX = Ctx;
    fn new_ctx(&self) -> Ctx {
        Ctx::default()
    }

    async fn upstream_peer(&self, _session: &mut Session, ctx: &mut Ctx) -> Result<Box<HttpPeer>> {
        ctx.attempt += 1;
        let mut peer = Box::new(HttpPeer::new(
            format!("127.0.0.1:{}", self.origin_port),
            false,
            String::new(),
        ));
        peer.options.read_timeout = Some(self.read_timeout);
        peer.options.response_header_timeout = self.header_timeout;
        Ok(peer)
    }

    async fn upstream_response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Ctx,
    ) -> Result<()> {
        ctx.status = upstream_response.status.as_u16();
        Ok(())
    }

    /// A body that ends without data after an upstream's 204 says so: what the application does
    /// with an upstream error that has no body.
    async fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end_of_stream: bool,
        ctx: &mut Ctx,
    ) -> Result<Option<Duration>> {
        if end_of_stream && body.is_none() && ctx.status == 204 {
            *body = Some(bytes::Bytes::from_static(b"no content"));
        }
        Ok(None)
    }

    fn upstream_response_wait(&self, _session: &Session, _ctx: &Ctx) -> Option<Duration> {
        self.wait
    }

    async fn upstream_response_waiting(
        &self,
        session: &mut Session,
        _ctx: &mut Ctx,
    ) -> Result<Option<Duration>> {
        if session.response_written().is_none() {
            let mut h = ResponseHeader::build(200, None)?;
            h.insert_header("Content-Type", "text/event-stream")?;
            h.insert_header("Transfer-Encoding", "chunked")?;
            session.write_response_header(Box::new(h), false).await?;
            self.provisional_headers.fetch_add(1, Ordering::SeqCst);
        }
        session
            .write_response_body(Some(bytes::Bytes::from_static(b": wait\n\n")), false)
            .await?;
        Ok(Some(self.every))
    }

    /// The first attempt that loses its connection is repeated, as an application does with
    /// another upstream.
    fn error_while_proxy(
        &self,
        _peer: &HttpPeer,
        _session: &mut Session,
        mut e: Box<Error>,
        ctx: &mut Ctx,
        _client_reused: bool,
    ) -> Box<Error> {
        if ctx.attempt == 1 && !matches!(e.etype(), ErrorType::ReadTimedout) {
            e.set_retry(true);
        }
        e
    }

    async fn fail_to_proxy(&self, session: &mut Session, e: &Error, _ctx: &mut Ctx) -> FailToProxy {
        let code = match e.etype() {
            ErrorType::ReadTimedout => 504,
            _ => 502,
        };
        if session.response_written().is_none() {
            let _ = session.respond_error(code).await;
        }
        FailToProxy {
            error_code: code,
            can_reuse_downstream: false,
        }
    }
}

async fn spawn_proxy(app: WaitProxy) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::thread::spawn(move || {
        let mut server = pingora_core::server::Server::new(None).unwrap();
        server.bootstrap();
        let conf = Arc::new(ServerConf::default());
        let mut service = pingora_proxy::http_proxy_service(&conf, app);
        service.add_tcp(&format!("127.0.0.1:{port}"));
        server.add_service(service);
        server.run_forever();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("proxy did not start on 127.0.0.1:{port}");
}

/// The whole raw response (head and body as they came), read until the proxy closes.
async fn get(port: u16) -> String {
    tokio::time::timeout(CLIENT_TIMEOUT, async {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(b"GET /slow HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out).await;
        String::from_utf8_lossy(&out).into_owned()
    })
    .await
    .expect("the proxy did not finish the response")
}

struct Run {
    response: String,
    provisional_headers: usize,
    connections: usize,
}

async fn run(
    o: Origin,
    read_timeout: Duration,
    header_timeout: Option<Duration>,
    wait: Option<Duration>,
) -> Run {
    let connections = Arc::new(AtomicUsize::new(0));
    let origin_port = spawn_origin(o, connections.clone()).await;
    let provisional = Arc::new(AtomicUsize::new(0));
    let port = spawn_proxy(WaitProxy {
        origin_port,
        read_timeout,
        header_timeout,
        wait,
        every: Duration::from_millis(100),
        provisional_headers: provisional.clone(),
    })
    .await;
    let response = get(port).await;
    Run {
        response,
        provisional_headers: provisional.load(Ordering::SeqCst),
        connections: connections.load(Ordering::SeqCst),
    }
}

fn late(header_delay_ms: u64) -> Origin {
    Origin {
        header_delay: Duration::from_millis(header_delay_ms),
        body_delay: Duration::ZERO,
        head: "HTTP/1.1 200 OK\r\nContent-Length: 5",
        body: "hello",
        drop_first: false,
    }
}

#[tokio::test]
async fn a_late_header_is_waited_for_with_its_own_timeout() {
    // Without it, the header is read with read_timeout and the request fails.
    let r = run(late(700), Duration::from_millis(300), None, None).await;
    assert!(r.response.starts_with("HTTP/1.1 504"), "{}", r.response);

    let r = run(
        late(700),
        Duration::from_millis(300),
        Some(Duration::from_secs(3)),
        None,
    )
    .await;
    assert!(r.response.starts_with("HTTP/1.1 200"), "{}", r.response);
    assert!(r.response.ends_with("hello"), "{}", r.response);
}

#[tokio::test]
async fn the_body_is_read_with_read_timeout_once_the_header_is_in() {
    let o = Origin {
        header_delay: Duration::ZERO,
        body_delay: Duration::from_millis(700),
        ..late(0)
    };
    let r = run(
        o,
        Duration::from_millis(300),
        Some(Duration::from_secs(3)),
        None,
    )
    .await;
    assert!(r.response.starts_with("HTTP/1.1 200"), "{}", r.response);
    assert!(!r.response.contains("hello"), "{}", r.response);
}

#[tokio::test]
async fn the_client_is_answered_while_the_header_comes() {
    let r = run(
        late(700),
        Duration::from_millis(300),
        Some(Duration::from_secs(3)),
        Some(Duration::from_millis(100)),
    )
    .await;
    let resp = &r.response;
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(resp.contains("text/event-stream"), "{resp}");
    // The upstream's header went through the filters but not to the client.
    assert!(!resp.contains("x-origin"), "{resp}");
    assert_eq!(resp.matches("HTTP/1.1").count(), 1, "{resp}");
    assert!(resp.matches(": wait").count() >= 3, "{resp}");
    // The upstream's body follows the provisional header, in its chunked framing, and ends it.
    assert!(resp.contains("5\r\nhello\r\n"), "{resp}");
    assert!(resp.ends_with("0\r\n\r\n"), "{resp}");
    assert_eq!(r.provisional_headers, 1);
}

#[tokio::test]
async fn a_header_that_ends_the_response_ends_the_body() {
    let o = Origin {
        head: "HTTP/1.1 204 No Content",
        body: "",
        ..late(400)
    };
    let r = run(
        o,
        Duration::from_millis(300),
        Some(Duration::from_secs(3)),
        Some(Duration::from_millis(100)),
    )
    .await;
    let resp = &r.response;
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(resp.contains(": wait"), "{resp}");
    // The body filters saw that end, as the end of a body.
    assert!(resp.contains("A\r\nno content\r\n"), "{resp}");
    assert!(resp.ends_with("0\r\n\r\n"), "{resp}");
}

#[tokio::test]
async fn a_retry_after_a_provisional_answer_continues_it() {
    let o = Origin {
        drop_first: true,
        ..late(400)
    };
    let r = run(
        o,
        Duration::from_millis(300),
        Some(Duration::from_secs(3)),
        Some(Duration::from_millis(100)),
    )
    .await;
    let resp = &r.response;
    assert_eq!(r.connections, 2, "{resp}");
    assert_eq!(r.provisional_headers, 1, "{resp}");
    assert_eq!(resp.matches("HTTP/1.1").count(), 1, "{resp}");
    assert!(resp.contains("5\r\nhello\r\n"), "{resp}");
    assert!(resp.ends_with("0\r\n\r\n"), "{resp}");
}

#[tokio::test]
async fn a_final_header_behind_a_1xx_ends_the_wait() {
    // The 100 and the final header arrive together: the final one is not the first task.
    let o = Origin {
        head: "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5",
        body_delay: Duration::from_millis(1000),
        ..late(250)
    };
    let r = run(
        o,
        Duration::from_secs(3),
        Some(Duration::from_secs(3)),
        Some(Duration::from_millis(100)),
    )
    .await;
    let resp = &r.response;
    assert!(resp.contains("5\r\nhello\r\n"), "{resp}");
    // Called while the header was on its way (~250 ms), not during the body's second.
    assert!(resp.matches(": wait").count() <= 4, "{resp}");
}
