#![cfg(all(
    feature = "transport-streamable-http-client",
    feature = "transport-streamable-http-client-reqwest",
    not(feature = "local")
))]
//! The reqwest backend reads non-SSE POST bodies only up to a fixed size: JSON
//! success bodies up to 16 MiB, error bodies up to 64 KiB. An oversized body
//! fails closed: no prefix is parsed and a notification is not reported as
//! accepted. The client must also stop reading a body that is still open
//! once the limit is exceeded.
//!
//! A raw TCP fixture controls the framing exactly, including `Content-Length`
//! values that do not match the bytes sent, chunked bodies without a final
//! chunk, and connections kept open after the body.

use std::{collections::HashMap, fmt::Debug, io, sync::Arc, time::Duration};

use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientNotification, ClientRequest, InitializedNotification,
        JsonRpcMessage, PingRequest, RequestId,
    },
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

/// Must match the private limits in the reqwest backend.
const MAX_JSON_BODY: usize = 16 * 1024 * 1024;
const MAX_ERROR_BODY: usize = 64 * 1024;
/// Far above any limit; never sent, only declared.
const HUGE_DECLARED: u64 = 1 << 40;
/// Generous; a bounded read finishes in well under a second.
const DEADLINE: Duration = Duration::from_secs(10);
const WRITE_CHUNK: usize = 64 * 1024;

const RESULT_JSON: &str = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
const ERROR_JSON: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"bounded-error"}}"#;
const TEXT: &str = "private-error-body";
const CHALLENGE: &str = "Bearer realm=\"test\"";

type PostResult = Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>>;

#[derive(Clone, Copy, Debug)]
enum Framing {
    /// `Content-Length` equal to the bytes sent.
    Length,
    /// `Content-Length` with this value, whatever is sent.
    Declared(u64),
    /// Chunked; the final chunk is sent only with [`End::Close`].
    Chunked,
}

#[derive(Clone, Copy, Debug)]
enum End {
    /// Finish the framing and close the connection.
    Close,
    /// Keep the connection open without finishing the body.
    Hold,
}

#[derive(Clone, Copy, Debug)]
enum Payload {
    /// Valid JSON-RPC response, padded with trailing whitespace.
    Result,
    /// Valid JSON-RPC error, padded with trailing whitespace.
    Error,
    /// Plain text.
    Text,
}

impl Payload {
    /// `len` bytes: a valid document followed by padding, so any prefix that
    /// covers the document still parses. A length of 0 sends nothing.
    fn bytes(self, len: usize) -> Arc<[u8]> {
        if len == 0 {
            return Arc::from(Vec::new());
        }
        let (prefix, fill) = match self {
            Payload::Result => (RESULT_JSON, b' '),
            Payload::Error => (ERROR_JSON, b' '),
            Payload::Text => (TEXT, b'x'),
        };
        assert!(prefix.len() <= len);
        let mut body = Vec::with_capacity(len);
        body.extend_from_slice(prefix.as_bytes());
        body.resize(len, fill);
        body.into()
    }
}

#[derive(Clone, Copy, Debug)]
enum Message {
    Request,
    Notification,
}

impl Message {
    fn build(self) -> ClientJsonRpcMessage {
        match self {
            Message::Request => ClientJsonRpcMessage::request(
                ClientRequest::PingRequest(PingRequest::default()),
                RequestId::Number(1),
            ),
            Message::Notification => ClientJsonRpcMessage::notification(
                ClientNotification::InitializedNotification(InitializedNotification::default()),
            ),
        }
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    challenge: Option<&'static str>,
    body: Arc<[u8]>,
    framing: Framing,
    end: End,
}

impl Reply {
    fn new(status: u16, content_type: &'static str, body: Arc<[u8]>) -> Self {
        Self {
            status,
            content_type,
            challenge: None,
            body,
            framing: Framing::Length,
            end: End::Close,
        }
    }

    fn framed(mut self, framing: Framing, end: End) -> Self {
        self.framing = framing;
        self.end = end;
        self
    }
}

struct RawServer {
    uri: Arc<str>,
    task: JoinHandle<()>,
}

impl Drop for RawServer {
    fn drop(&mut self) {
        // Dropping the JoinSet inside the task aborts every connection.
        self.task.abort();
    }
}

async fn serve(reply: Reply) -> RawServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reply = Arc::new(reply);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        while let Ok((stream, _)) = listener.accept().await {
            connections.spawn(respond(stream, reply.clone()));
        }
    });
    RawServer {
        uri: Arc::from(format!("http://{address}/mcp")),
        task,
    }
}

async fn respond(mut stream: TcpStream, reply: Arc<Reply>) {
    if read_request(&mut stream).await.is_err() {
        return;
    }
    // A client that stops reading may reset the connection mid-write.
    let _ = write_reply(&mut stream, &reply).await;
    if matches!(reply.end, End::Hold) {
        std::future::pending::<()>().await;
    }
}

/// Reads one request head and its `Content-Length` body.
async fn read_request(stream: &mut TcpStream) -> io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_ascii_lowercase();
    let body_len = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while buffer.len() < head_end + body_len {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(())
}

async fn write_reply(stream: &mut TcpStream, reply: &Reply) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} Test\r\ncontent-type: {}\r\nretry-after: 17\r\n",
        reply.status, reply.content_type
    );
    if let Some(challenge) = reply.challenge {
        head.push_str(&format!("www-authenticate: {challenge}\r\n"));
    }
    match reply.framing {
        Framing::Length => head.push_str(&format!("content-length: {}\r\n", reply.body.len())),
        Framing::Declared(len) => head.push_str(&format!("content-length: {len}\r\n")),
        Framing::Chunked => head.push_str("transfer-encoding: chunked\r\n"),
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    let chunked = matches!(reply.framing, Framing::Chunked);
    for piece in reply.body.chunks(WRITE_CHUNK) {
        if chunked {
            stream
                .write_all(format!("{:x}\r\n", piece.len()).as_bytes())
                .await?;
            stream.write_all(piece).await?;
            stream.write_all(b"\r\n").await?;
        } else {
            stream.write_all(piece).await?;
        }
    }
    if chunked && matches!(reply.end, End::Close) {
        stream.write_all(b"0\r\n\r\n").await?;
    }
    stream.flush().await
}

async fn post(server: &RawServer, message: Message, session: Option<&str>) -> PostResult {
    tokio::time::timeout(
        DEADLINE,
        reqwest::Client::new().post_message(
            server.uri.clone(),
            message.build(),
            session.map(Arc::from),
            None,
            HashMap::new(),
        ),
    )
    .await
    .expect("the POST must finish without reading past the body limit")
}

/// Bounded `Debug` output, so a failure never dumps a large body.
fn summary(value: &impl Debug) -> String {
    const MAX: usize = 300;
    let rendered = format!("{value:?}");
    if rendered.len() <= MAX {
        return rendered;
    }
    let mut end = MAX;
    while !rendered.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... ({} bytes)", &rendered[..end], rendered.len())
}

#[tokio::test]
async fn oversized_bodies_reject_without_eof_or_parsing_a_valid_prefix() {
    for (status, content_type, limit, payload) in [
        (200, "application/json", MAX_JSON_BODY, Payload::Result),
        (400, "application/json", MAX_ERROR_BODY, Payload::Error),
        (500, "application/json", MAX_ERROR_BODY, Payload::Error),
        (400, "text/plain", MAX_ERROR_BODY, Payload::Text),
        (500, "text/plain", MAX_ERROR_BODY, Payload::Text),
    ] {
        for message in [Message::Request, Message::Notification] {
            for (framing, end, size) in [
                (Framing::Length, End::Close, limit + 1),
                (Framing::Chunked, End::Hold, limit + 1),
                (Framing::Declared(HUGE_DECLARED), End::Hold, 0),
            ] {
                let server = serve(
                    Reply::new(status, content_type, payload.bytes(size)).framed(framing, end),
                )
                .await;
                let result = post(&server, message, None).await;
                assert!(
                    matches!(result, Err(StreamableHttpError::ResponseBodyTooLarge { max_size }) if max_size == limit),
                    "{status} {message:?} {framing:?}: {}",
                    summary(&result)
                );
            }
        }
    }
}

#[tokio::test]
async fn bodies_at_the_limit_preserve_json_and_error_results() {
    for framing in [Framing::Length, Framing::Chunked] {
        for message in [Message::Request, Message::Notification] {
            let server = serve(
                Reply::new(
                    200,
                    "application/json",
                    Payload::Result.bytes(MAX_JSON_BODY),
                )
                .framed(framing, End::Close),
            )
            .await;
            let result = post(&server, message, None).await;
            assert!(
                matches!(
                    result,
                    Ok(StreamableHttpPostResponse::Json(
                        JsonRpcMessage::Response(_),
                        _
                    ))
                ),
                "{}",
                summary(&result)
            );
            for status in [400, 500] {
                let server = serve(
                    Reply::new(
                        status,
                        "application/json",
                        Payload::Error.bytes(MAX_ERROR_BODY),
                    )
                    .framed(framing, End::Close),
                )
                .await;
                let result = post(&server, message, None).await;
                assert!(
                    matches!(
                        result,
                        Ok(StreamableHttpPostResponse::Json(
                            JsonRpcMessage::Error(_),
                            _
                        ))
                    ),
                    "{}",
                    summary(&result)
                );
            }
        }
    }
}

#[tokio::test]
async fn body_free_status_and_auth_paths_keep_existing_behavior() {
    for status in [202, 204, 404, 401] {
        let mut reply = Reply::new(status, "application/json", Payload::Text.bytes(0))
            .framed(Framing::Declared(HUGE_DECLARED), End::Hold);
        if status == 401 {
            reply.challenge = Some(CHALLENGE);
        }
        let server = serve(reply).await;
        let result = post(&server, Message::Request, Some("existing-session")).await;
        assert!(
            match (status, &result) {
                (202 | 204, Ok(StreamableHttpPostResponse::Accepted)) => true,
                (404, Err(StreamableHttpError::SessionExpired)) => true,
                (401, Err(StreamableHttpError::AuthRequired(_))) => true,
                _ => false,
            },
            "{status}: {}",
            summary(&result)
        );
    }
}

#[tokio::test]
async fn bounded_error_decoding_matches_reqwest_charset_and_bom_handling() {
    for (content_type, bytes) in [
        (
            "text/plain; charset=windows-1252",
            b"caf\xe9 \x80".as_slice(),
        ),
        (
            "text/plain; charset=shift_jis",
            b"\x82\xb1\x82\xf1\x82\xc9\x82\xbf\x82\xcd".as_slice(),
        ),
        (
            "text/plain; charset=windows-1252",
            b"\xff\xfec\0a\0f\0\xe9\0".as_slice(),
        ),
        ("text/plain", b"\xef\xbb\xbfcaf\xc3\xa9".as_slice()),
        ("text/plain; charset=utf-8", b"bad \xff".as_slice()),
        (
            "application/json; charset=windows-1252",
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32603,\"message\":\"caf\xe9\"}}"
                .as_slice(),
        ),
    ] {
        let server = serve(Reply::new(400, content_type, Arc::from(bytes))).await;
        let expected = reqwest::Client::new()
            .post(server.uri.as_ref())
            .json(&Message::Request.build())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let result = post(&server, Message::Request, None).await;
        if content_type.starts_with("application/json")
            && let Ok(JsonRpcMessage::Error(error)) =
                serde_json::from_str::<rmcp::model::ServerJsonRpcMessage>(&expected)
        {
            match result {
                Ok(StreamableHttpPostResponse::Json(JsonRpcMessage::Error(actual), _)) => {
                    assert_eq!(actual.error, error.error)
                }
                other => panic!("expected decoded JSON error, got {}", summary(&other)),
            }
        } else {
            match result {
                Err(StreamableHttpError::UnexpectedServerResponse(actual)) => {
                    assert_eq!(actual, format!("HTTP 400 Bad Request: {expected}"))
                }
                other => panic!("expected decoded HTTP error, got {}", summary(&other)),
            }
        }
    }
}

#[tokio::test]
async fn charset_limits_count_wire_bytes_before_text_expansion() {
    for size in [MAX_ERROR_BODY, MAX_ERROR_BODY + 1] {
        let server = serve(Reply::new(
            400,
            "text/plain; charset=windows-1252",
            vec![0x80; size].into(),
        ))
        .await;
        let result = post(&server, Message::Request, None).await;
        if size > MAX_ERROR_BODY {
            assert!(
                matches!(
                    result,
                    Err(StreamableHttpError::ResponseBodyTooLarge {
                        max_size: MAX_ERROR_BODY
                    })
                ),
                "{}",
                summary(&result)
            );
        } else {
            let expected = reqwest::Client::new()
                .post(server.uri.as_ref())
                .json(&Message::Request.build())
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            match result {
                Err(StreamableHttpError::UnexpectedServerResponse(actual)) => {
                    assert_eq!(actual, format!("HTTP 400 Bad Request: {expected}"))
                }
                other => panic!("expected bounded decoded error, got {}", summary(&other)),
            }
        }
    }
}
