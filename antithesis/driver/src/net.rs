//! UDP receivers for client events and forwarded logs, the HTTP client used to drive the
//! client programs, and a raw MQTT CONNECT probe for broker readiness.
use crate::model::Model;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long an out-of-order client event waits for the gap before it to fill.
const REORDER_WAIT: Duration = Duration::from_millis(500);

struct Reorder {
    expected: u64,
    buffered: BTreeMap<u64, (Value, Instant)>,
}

/// Receives client events on `port`, puts each client's events back in `seq` order, and
/// applies them to the model.
pub fn start_event_receiver(port: u16, model: Arc<Mutex<Model>>, names: HashMap<String, usize>) {
    let socket = UdpSocket::bind(("0.0.0.0", port)).expect("bind event port");
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("socket timeout");
    std::thread::Builder::new()
        .name("events".into())
        .spawn(move || {
            let mut buf = vec![0u8; 65536];
            let mut streams: HashMap<(usize, u64), Reorder> = HashMap::new();
            loop {
                match socket.recv_from(&mut buf) {
                    Ok((n, _)) => {
                        model.lock().unwrap().stats.packets += 1;
                        // A datagram carries one or more newline-separated events.
                        for line in buf[..n].split(|&b| b == b'\n') {
                            if line.is_empty() {
                                continue;
                            }
                            let Ok(v) = serde_json::from_slice::<Value>(line) else {
                                model.lock().unwrap().stats.bad_events += 1;
                                continue;
                            };
                            let Some(idx) = v["client"].as_str().and_then(|c| names.get(c)).copied() else {
                                model.lock().unwrap().stats.bad_events += 1;
                                continue;
                            };
                            let boot = v["boot"].as_u64().unwrap_or(0);
                            let seq = v["seq"].as_u64().unwrap_or(0);
                            let stream = streams.entry((idx, boot)).or_insert(Reorder {
                                expected: seq,
                                buffered: BTreeMap::new(),
                            });
                            if seq < stream.expected {
                                model.lock().unwrap().stats.dup_events += 1;
                                continue;
                            }
                            stream.buffered.insert(seq, (v, Instant::now()));
                            drain(stream, idx, &model, false);
                        }
                    }
                    Err(_) => {
                        for ((idx, _), stream) in streams.iter_mut() {
                            drain(stream, *idx, &model, true);
                        }
                    }
                }
            }
        })
        .expect("spawn events thread");
}

fn drain(stream: &mut Reorder, idx: usize, model: &Arc<Mutex<Model>>, timed: bool) {
    loop {
        let Some((&seq, (_, arrived))) = stream.buffered.iter().next() else { return };
        if seq != stream.expected {
            let waited = arrived.elapsed();
            if timed || waited >= REORDER_WAIT || stream.buffered.len() > 2000 {
                if waited < REORDER_WAIT && stream.buffered.len() <= 2000 {
                    return;
                }
                let mut m = model.lock().unwrap();
                m.stats.lost_events += seq - stream.expected;
                stream.expected = seq;
            } else {
                return;
            }
        }
        let (v, _) = stream.buffered.remove(&seq).unwrap();
        stream.expected = seq + 1;
        model.lock().unwrap().on_client_event(idx, &v);
    }
}

/// Receives forwarded log lines (`{"node","boot","seq","line"}`) on `port`.
pub fn start_log_receiver(port: u16, model: Arc<Mutex<Model>>) {
    let socket = UdpSocket::bind(("0.0.0.0", port)).expect("bind log port");
    std::thread::Builder::new()
        .name("logs".into())
        .spawn(move || {
            let mut buf = vec![0u8; 65536];
            loop {
                let Ok((n, _)) = socket.recv_from(&mut buf) else { continue };
                let Ok(v) = serde_json::from_slice::<Value>(&buf[..n]) else { continue };
                let node = v["node"].as_str().unwrap_or("").to_string();
                let boot = v["boot"].as_u64().unwrap_or(0);
                let seq = v["seq"].as_u64().unwrap_or(0);
                let line = v["line"].as_str().unwrap_or("").to_string();
                model.lock().unwrap().on_log_line(&node, boot, seq, &line);
            }
        })
        .expect("spawn logs thread");
}

pub fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(1))
        .timeout(Duration::from_secs(3))
        .build()
}

pub fn post_op(agent: &ureq::Agent, url: &str, body: &Value) -> Result<Value, String> {
    match agent.post(&format!("{url}/op")).send_json(body) {
        Ok(resp) => resp.into_json::<Value>().map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

pub fn get_health(agent: &ureq::Agent, url: &str) -> Result<Value, String> {
    match agent.get(&format!("{url}/health")).call() {
        Ok(resp) => resp.into_json::<Value>().map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

/// Opens a TCP connection, sends an MQTT 3.1.1 CONNECT, and expects a CONNACK with return
/// code 0. Used only for readiness; actions go through the client programs.
pub fn mqtt_probe(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let addr = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve: {e}"))?
        .next()
        .ok_or_else(|| "resolve: no address".to_string())?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    let client_id = b"driver-probe";
    // Fixed header + variable header (protocol name, level 4, flags clean session, keepalive 10)
    let mut body = Vec::new();
    body.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x0a]);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id);
    let mut packet = vec![0x10, body.len() as u8];
    packet.extend_from_slice(&body);
    stream.write_all(&packet).map_err(|e| format!("write: {e}"))?;
    let mut ack = [0u8; 4];
    stream.read_exact(&mut ack).map_err(|e| format!("read: {e}"))?;
    if ack[0] != 0x20 || ack[3] != 0 {
        return Err(format!("bad connack {ack:?}"));
    }
    // DISCONNECT, so the probe's session doesn't linger.
    let _ = stream.write_all(&[0xe0, 0x00]);
    Ok(())
}
