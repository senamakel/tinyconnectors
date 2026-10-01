//! Loopback fixtures shared by the HTTP-level tests: a canned server and a
//! CONNECT proxy that really tunnels, so a test can assert traffic went
//! through it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

/// What a server answers: status line, extra headers, body.
pub(crate) type Reply = (u16, Vec<(&'static str, String)>, String);

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
            let head = read_head(&stream);
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
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let blank = line == "\r\n";
        head.push_str(&line);
        if blank {
            break;
        }
    }
    head
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
