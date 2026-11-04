//! Copies stdin to stdout line by line and forwards every line to the driver over UDP as
//! `{"node": <hostname>, "boot": <id>, "seq": <n>, "line": <text>}`. Never blocks or exits
//! on network errors: lines that can't be sent are dropped.
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn main() {
    let node = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    let target = std::env::var("LOG_FORWARD_ADDR").ok();
    let boot = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let socket = UdpSocket::bind("0.0.0.0:0").ok();
    let mut resolved: Option<SocketAddr> = None;
    let mut last_resolve = Instant::now() - Duration::from_secs(10);

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut seq: u64 = 0;
    let mut buf = Vec::new();
    let mut reader = stdin.lock();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim_end_matches(['\n', '\r']);
        let _ = out.write_all(line.as_bytes());
        let _ = out.write_all(b"\n");
        let _ = out.flush();

        if let (Some(target), Some(socket)) = (&target, &socket) {
            if resolved.is_none() && last_resolve.elapsed() >= Duration::from_secs(1) {
                last_resolve = Instant::now();
                resolved = target.to_socket_addrs().ok().and_then(|mut a| a.next());
            }
            if let Some(addr) = resolved {
                let msg = format!(
                    "{{\"node\":\"{}\",\"boot\":{},\"seq\":{},\"line\":\"{}\"}}",
                    json_escape(&node),
                    boot,
                    seq,
                    json_escape(line)
                );
                // Keep datagrams under the usual MTU-ish ceiling; long lines are truncated.
                let bytes = msg.as_bytes();
                let bytes = if bytes.len() > 8000 { &bytes[..8000] } else { bytes };
                let _ = socket.send_to(bytes, addr);
            }
        }
        seq += 1;
    }
}
