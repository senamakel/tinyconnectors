//! Loopback fixtures shared by the HTTP-level tests: a canned server and a
//! CONNECT proxy that really tunnels, so a test can assert traffic went
//! through it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::format_push_string
)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

/// What a server answers: status line, extra headers, body.
pub(crate) type Reply = (u16, Vec<(&'static str, String)>, String);

/// A reserved loopback endpoint whose first connection closes without an HTTP response.
pub(crate) struct NoResponseEndpoint {
    address: SocketAddr,
    _listener: TcpListener,
    rejection: Option<std::thread::JoinHandle<()>>,
}

impl NoResponseEndpoint {
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for NoResponseEndpoint {
    fn drop(&mut self) {
        if let Some(rejection) = self.rejection.take() {
            if !rejection.is_finished() {
                let _ = TcpStream::connect(self.address);
            }
            let _ = rejection.join();
        }
    }
}

/// Keep the destination port occupied through the request, then close its
/// first connection without an HTTP response.
pub(crate) fn closed_loopback_endpoint_without_response() -> NoResponseEndpoint {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = listener.try_clone().unwrap();
    let rejection = std::thread::spawn(move || {
        if let Ok((stream, _)) = acceptor.accept() {
            drop(stream);
        }
    });
    NoResponseEndpoint {
        address,
        _listener: listener,
        rejection: Some(rejection),
    }
}

/// A loopback HTTP server answering every request with `respond(request_head)`.
///
/// Returns the base URL and every request head it saw.
pub(crate) fn server(
    respond: impl Fn(&str) -> Reply + Send + Sync + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            // Drain request bodies before replying. Closing a TCP stream with
            // unread POST bytes can reset the response on Windows.
            let head = read_request(&stream).unwrap_or_default();
            if head.is_empty() {
                continue;
            }
            log.lock().unwrap().push(head.clone());
            let (status, headers, body) = respond(&head);
            let mut out = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\n", body.len());
            for (name, value) in headers {
                out.push_str(&format!("{name}: {value}\r\n"));
            }
            out.push_str("Connection: close\r\n\r\n");
            out.push_str(&body);
            let mut stream = &stream;
            let _ = stream.write_all(out.as_bytes());
            let _ = stream.flush();
        }
    });
    (format!("http://127.0.0.1:{port}/api/v3"), seen)
}

pub(crate) fn read_head(stream: &TcpStream) -> String {
    let mut reader = BufReader::new(stream);
    read_head_from(&mut reader).unwrap_or_default()
}

fn read_request(stream: &TcpStream) -> io::Result<String> {
    let mut reader = BufReader::new(stream);
    read_request_from(&mut reader)
}

pub(crate) fn read_request_from(reader: &mut impl BufRead) -> io::Result<String> {
    let head = read_head_from(reader)?;
    if head.is_empty() {
        return Ok(head);
    }

    let content_length = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    if let Some(length) = content_length {
        let drained = io::copy(&mut (&mut *reader).take(length as u64), &mut io::sink())?;
        if drained != length as u64 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request ended before its declared body length",
            ));
        }
    }
    Ok(head)
}

fn read_head_from(reader: &mut impl BufRead) -> io::Result<String> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let blank = line == "\r\n";
        head.push_str(&line);
        if blank {
            break;
        }
    }
    Ok(head)
}

pub(crate) fn json(body: &str) -> Reply {
    (200, Vec::new(), body.to_string())
}

/// A CONNECT proxy on loopback that tunnels to whatever it is asked for and
/// reports each CONNECT line it saw.
pub(crate) fn connect_proxy() -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut client) = stream else { return };
            let head = read_head(&client);
            let Some(line) = head.lines().next().map(str::to_string) else {
                continue;
            };
            let _ = sender.send(head.clone());
            let Some(target) = line
                .strip_prefix("CONNECT ")
                .and_then(|r| r.split(' ').next())
            else {
                continue;
            };
            let Ok(upstream) = TcpStream::connect(target) else {
                let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n");
                continue;
            };
            let _ = client.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n");
            let (mut client_read, mut upstream_write) =
                (client.try_clone().unwrap(), upstream.try_clone().unwrap());
            std::thread::spawn(move || {
                let _ = std::io::copy(&mut client_read, &mut upstream_write);
            });
            let mut upstream_read = upstream;
            let _ = std::io::copy(&mut upstream_read, &mut client);
        }
    });
    (format!("http://127.0.0.1:{port}"), receiver)
}
