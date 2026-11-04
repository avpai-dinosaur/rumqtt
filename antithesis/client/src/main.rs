//! One long-lived rumqttc client, driven over HTTP by the Antithesis driver.
//!
//! The driver sends operations to `POST /op`; this program runs them on one shared
//! `AsyncClient` and reports every completion (acks, SUBACKs, deliveries, connection
//! changes, wire sends) as a JSON event over UDP to `OUTCOME_ADDR`. `GET /health`
//! answers without touching the broker.
use antithesis_instrumentation as _;

use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, LastWill, MqttOptions, Outgoing, Packet, QoS,
    Request,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Cmd {
    Publish {
        id: u64,
        topic: String,
        qos: u8,
        retain: bool,
        /// Filler bytes after the header; 0 with `empty` makes a zero-byte payload.
        size: usize,
        #[serde(default)]
        empty: bool,
    },
    Subscribe {
        id: u64,
        filter: String,
        qos: u8,
    },
    Unsubscribe {
        id: u64,
        filter: String,
    },
    Disconnect {
        id: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Publish,
    Subscribe,
    Unsubscribe,
    Disconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpState {
    /// Handed to the event loop's request channel, no packet id yet.
    Queued,
    /// Parked by a packet-id collision; it goes out when the slot frees.
    AwaitingSlot,
    /// On the wire at least once.
    Sent,
}

struct Op {
    id: u64,
    kind: OpKind,
    qos: u8,
    state: OpState,
    sends: u32,
    done: Arc<Notify>,
}

struct Inner {
    /// Ops in the request channel (or the event loop's `pending`), in submission order.
    /// The next assignment event (`Outgoing::Publish(0)`, `Outgoing::Subscribe`, ...)
    /// belongs to the head.
    pending_assign: VecDeque<Op>,
    /// Ops that hold a packet id, per packet id; a collision queues a second one.
    by_pkid: HashMap<u16, VecDeque<Op>>,
    /// How many ops at the head of `pending_assign` sit in the event loop's `pending`
    /// list after a disconnect. They're re-sent if the session resumes, and dropped if not.
    at_risk: usize,
    connected: bool,
    /// Whether this process has completed a CONNECT before (stale ops exist only then).
    prev_connack: bool,
}

struct Shared {
    client_id: String,
    boot: u64,
    start: Instant,
    seq: AtomicU64,
    attempt: AtomicU64,
    conn: AtomicU64,
    connected: AtomicBool,
    session_present: AtomicBool,
    clean: bool,
    udp: Option<UdpSocket>,
    /// Event lines waiting to be sent in one datagram.
    batch: Mutex<Vec<u8>>,
    outcome: Mutex<(Option<SocketAddr>, Option<Instant>)>,
    outcome_host: String,
    inner: Mutex<Inner>,
    client: AsyncClient,
}

#[derive(Serialize)]
struct Health<'a> {
    client: &'a str,
    boot: u64,
    conn: u64,
    attempt: u64,
    connected: bool,
    sp: bool,
}

impl Shared {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Sends one event to the driver, and mirrors it on stderr.
    fn event(&self, mut v: serde_json::Value) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        if let serde_json::Value::Object(ref mut m) = v {
            m.insert("client".into(), self.client_id.clone().into());
            m.insert("boot".into(), self.boot.into());
            m.insert("seq".into(), seq.into());
            m.insert("t".into(), self.now_ms().into());
            m.insert("conn".into(), self.conn.load(Ordering::SeqCst).into());
            m.insert("sp".into(), self.session_present.load(Ordering::SeqCst).into());
            m.insert("clean".into(), self.clean.into());
        }
        let line = v.to_string();
        eprintln!("{line}");
        let flush_now = {
            let mut batch = self.batch.lock().unwrap();
            batch.extend_from_slice(line.as_bytes());
            batch.push(b'\n');
            batch.len() > 6000
        };
        if flush_now {
            self.flush();
        }
    }

    /// Sends the buffered event lines, in datagrams of at most ~8 KB.
    fn flush(&self) {
        let data = {
            let mut batch = self.batch.lock().unwrap();
            if batch.is_empty() {
                return;
            }
            std::mem::take(&mut *batch)
        };
        let Some(udp) = &self.udp else { return };
        let addr = {
            let mut guard = self.outcome.lock().unwrap();
            if guard.0.is_none() {
                let due = guard.1.map_or(true, |t| t.elapsed() >= Duration::from_secs(1));
                if due {
                    guard.1 = Some(Instant::now());
                    guard.0 = self
                        .outcome_host
                        .to_socket_addrs()
                        .ok()
                        .and_then(|mut a| a.next());
                }
            }
            guard.0
        };
        let Some(addr) = addr else { return };
        let mut start = 0;
        while start < data.len() {
            let mut end = (start + 8000).min(data.len());
            if end < data.len() {
                // Split at a line boundary.
                match data[start..end].iter().rposition(|&b| b == b'\n') {
                    Some(p) => end = start + p + 1,
                    None => end = (start + 8000).min(data.len()),
                }
            }
            let _ = udp.send_to(&data[start..end], addr);
            start = end;
        }
    }

    fn op_result(&self, op: &Op, outcome: &str, extra: serde_json::Value) {
        let mut v = serde_json::json!({"k": "op", "op": op.id, "outcome": outcome, "qos": op.qos});
        if let (serde_json::Value::Object(m), serde_json::Value::Object(e)) = (&mut v, extra) {
            for (k, val) in e {
                m.insert(k, val);
            }
        }
        self.event(v);
    }
}

fn will_for(client_id: &str, boot: u64, attempt: u64) -> (LastWill, u8, bool) {
    let qos = (attempt % 3) as u8;
    let retain = attempt % 2 == 1;
    let payload = format!("w|{client_id}|{boot}|{attempt}|{qos}|{}", retain as u8);
    (
        LastWill::new(
            format!("will/{client_id}"),
            payload,
            qos_of(qos),
            retain,
        ),
        qos,
        retain,
    )
}

fn qos_of(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// `m|<op>|<client>|<topic>|<qos>|<retain>|<size>|<crc32 of filler>|<filler>`
fn build_payload(op: u64, client: &str, topic: &str, qos: u8, retain: bool, size: usize) -> Vec<u8> {
    let mut filler = Vec::with_capacity(size);
    let mut x = op.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    for _ in 0..size {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        filler.push(b'a' + ((x >> 33) % 26) as u8);
    }
    let crc = crc32fast::hash(&filler);
    let mut out = format!("m|{op}|{client}|{topic}|{qos}|{}|{size}|{crc}|", retain as u8).into_bytes();
    out.extend_from_slice(&filler);
    out
}

/// Parses a delivered payload back into the fields the driver needs, checking the filler.
fn parse_payload(payload: &[u8]) -> serde_json::Value {
    if payload.is_empty() {
        return serde_json::json!({"fmt": "empty"});
    }
    let text = String::from_utf8_lossy(payload);
    if text.starts_with("w|") {
        return serde_json::json!({"fmt": "will", "raw": text});
    }
    if !text.starts_with("m|") {
        return serde_json::json!({"fmt": "unknown", "raw": text.chars().take(80).collect::<String>()});
    }
    let mut parts = text.splitn(9, '|');
    let _m = parts.next();
    let op = parts.next().and_then(|s| s.parse::<u64>().ok());
    let client = parts.next().map(|s| s.to_string());
    let topic = parts.next().map(|s| s.to_string());
    let qos = parts.next().and_then(|s| s.parse::<u8>().ok());
    let retain = parts.next().and_then(|s| s.parse::<u8>().ok()).map(|v| v == 1);
    let size = parts.next().and_then(|s| s.parse::<usize>().ok());
    let crc = parts.next().and_then(|s| s.parse::<u32>().ok());
    let filler = parts.next().map(|s| s.as_bytes()).unwrap_or(&[]);
    let intact = match (size, crc) {
        (Some(size), Some(crc)) => filler.len() == size && crc32fast::hash(filler) == crc,
        _ => false,
    };
    serde_json::json!({
        "fmt": "msg", "op": op, "pub": client, "ptopic": topic, "pqos": qos,
        "pretain": retain, "size": size, "intact": intact && op.is_some(),
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    env_logger::init();
    let client_id = std::env::var("CLIENT_ID").expect("CLIENT_ID");
    let host = std::env::var("BROKER_HOST").unwrap_or_else(|_| "broker".into());
    let port: u16 = std::env::var("BROKER_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1883);
    let clean: bool = std::env::var("CLEAN_SESSION").map(|v| v == "true" || v == "1").unwrap_or(true);
    let keep_alive: u64 = std::env::var("KEEP_ALIVE_SECS").ok().and_then(|p| p.parse().ok()).unwrap_or(5);
    let listen = std::env::var("LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let outcome_host = std::env::var("OUTCOME_ADDR").unwrap_or_else(|_| "driver:7778".into());
    let boot = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(1);

    let mut options = MqttOptions::new(client_id.clone(), host, port);
    options
        .set_keep_alive(Duration::from_secs(keep_alive))
        .set_clean_session(clean)
        .set_max_packet_size(65536, 65536)
        .set_request_channel_capacity(10);
    let (will, _, _) = will_for(&client_id, boot, 1);
    options.set_last_will(will);
    let (client, eventloop) = AsyncClient::new(options, 10);

    let shared = Arc::new(Shared {
        client_id: client_id.clone(),
        boot,
        start: Instant::now(),
        seq: AtomicU64::new(0),
        attempt: AtomicU64::new(0),
        conn: AtomicU64::new(0),
        connected: AtomicBool::new(false),
        session_present: AtomicBool::new(false),
        clean,
        udp: UdpSocket::bind("0.0.0.0:0").ok(),
        batch: Mutex::new(Vec::new()),
        outcome: Mutex::new((None, None)),
        outcome_host,
        inner: Mutex::new(Inner {
            pending_assign: VecDeque::new(),
            by_pkid: HashMap::new(),
            at_risk: 0,
            connected: false,
            prev_connack: false,
        }),
        client,
    });
    shared.event(serde_json::json!({"k": "boot", "clean": clean}));

    // Bounded: a client that can't keep up answers 503 so the driver backs off instead of
    // piling up work that would hide how the live system behaves.
    let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>(64);
    let flusher = shared.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        loop {
            tick.tick().await;
            flusher.flush();
        }
    });
    tokio::spawn(submitter(shared.clone(), cmd_rx));
    tokio::spawn(event_loop(shared.clone(), eventloop));
    http_server(shared, cmd_tx, &listen).await;
}

/// Runs commands one at a time: hands each to the event loop and waits until the loop has
/// assigned it a packet id (or dropped it), so assignment events can be matched in order.
async fn submitter(shared: Arc<Shared>, mut rx: mpsc::Receiver<Cmd>) {
    while let Some(cmd) = rx.recv().await {
        let (id, kind, qos) = match &cmd {
            Cmd::Publish { id, qos, .. } => (*id, OpKind::Publish, *qos),
            Cmd::Subscribe { id, qos, .. } => (*id, OpKind::Subscribe, *qos),
            Cmd::Unsubscribe { id, .. } => (*id, OpKind::Unsubscribe, 0),
            Cmd::Disconnect { id } => (*id, OpKind::Disconnect, 0),
        };
        let done = Arc::new(Notify::new());
        let req = match &cmd {
            Cmd::Publish { id, topic, qos, retain, size, empty } => {
                let payload = if *empty {
                    Vec::new()
                } else {
                    build_payload(*id, &shared.client_id, topic, *qos, *retain, *size)
                };
                let mut p = rumqttc::Publish::new(topic.clone(), qos_of(*qos), payload);
                p.retain = *retain;
                Request::Publish(p)
            }
            Cmd::Subscribe { filter, qos, .. } => {
                Request::Subscribe(rumqttc::Subscribe::new(filter.clone(), qos_of(*qos)))
            }
            Cmd::Unsubscribe { filter, .. } => {
                Request::Unsubscribe(rumqttc::Unsubscribe::new(filter.clone()))
            }
            Cmd::Disconnect { .. } => Request::Disconnect(rumqttc::Disconnect),
        };
        // Register, then enqueue: the event loop can emit the assignment event the moment
        // the request is in the channel, and the handler must already find the op at the
        // head of `pending_assign`.
        let op = Op { id, kind, qos, state: OpState::Queued, sends: 0, done: done.clone() };
        shared.inner.lock().unwrap().pending_assign.push_back(op);
        let res = match req {
            Request::Publish(p) => {
                shared.client.publish(p.topic.clone(), p.qos, p.retain, p.payload.to_vec()).await
            }
            Request::Subscribe(s) => {
                let f = s.filters.into_iter().next().unwrap();
                shared.client.subscribe(f.path, f.qos).await
            }
            Request::Unsubscribe(u) => shared.client.unsubscribe(u.topics[0].clone()).await,
            Request::Disconnect(_) => shared.client.disconnect().await,
            _ => unreachable!(),
        };
        if res.is_err() {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(pos) = inner.pending_assign.iter().position(|o| o.id == id) {
                let op = inner.pending_assign.remove(pos).unwrap();
                drop(inner);
                shared.op_result(&op, "invalid", serde_json::json!({}));
            }
            continue;
        }
        // A request that never gets an assignment event means the bookkeeping lost it; give
        // up on it after a generous wait so the client keeps serving the driver.
        if tokio::time::timeout(Duration::from_secs(180), done.notified()).await.is_err() {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(pos) = inner.pending_assign.iter().position(|o| o.id == id) {
                let op = inner.pending_assign.remove(pos).unwrap();
                drop(inner);
                shared.event(serde_json::json!({"k": "stuck", "op": op.id}));
                shared.op_result(&op, "dropped", serde_json::json!({"stuck": true}));
            }
        }
    }
}

fn pending_unassigned(pending: &VecDeque<Request>) -> usize {
    pending
        .iter()
        .filter(|r| match r {
            Request::Publish(p) => p.pkid == 0,
            Request::Subscribe(_) | Request::Unsubscribe(_) | Request::Disconnect(_) => true,
            _ => false,
        })
        .count()
}

async fn event_loop(shared: Arc<Shared>, mut eventloop: EventLoop) {
    loop {
        if eventloop.network.is_none() {
            // Every CONNECT carries a will unique to this attempt, so a will delivery names
            // exactly one connection.
            let attempt = shared.attempt.fetch_add(1, Ordering::SeqCst) + 1;
            let (will, _, _) = will_for(&shared.client_id, shared.boot, attempt);
            eventloop.mqtt_options.set_last_will(will);
        }
        match eventloop.poll().await {
            Ok(Event::Incoming(packet)) => handle_incoming(&shared, &mut eventloop, packet),
            Ok(Event::Outgoing(out)) => handle_outgoing(&shared, out),
            Err(e) => {
                // rumqttc keeps packets it already read from the dead connection in its event
                // queue and would yield them after the next CONNACK; attribute them to this
                // connection now.
                let mut leftovers = Vec::new();
                while let Some(ev) = eventloop.state.events.pop_front() {
                    leftovers.push(ev);
                }
                for ev in leftovers {
                    match ev {
                        Event::Incoming(packet) => handle_incoming(&shared, &mut eventloop, packet),
                        Event::Outgoing(out) => handle_outgoing(&shared, out),
                    }
                }
                let was_connected = shared.connected.swap(false, Ordering::SeqCst);
                let reason = describe(&e);
                // Everything the loop pulled back into `pending` (unacked publishes, and the
                // requests it drained from the channel) is at risk of being discarded if the
                // next CONNACK doesn't resume the session. The drained requests are the
                // oldest unassigned ops, so they're a prefix of `pending_assign`.
                let at_risk = pending_unassigned(&eventloop.pending);
                {
                    let mut inner = shared.inner.lock().unwrap();
                    inner.connected = false;
                    inner.at_risk = at_risk.min(inner.pending_assign.len());
                }
                if was_connected {
                    shared.event(serde_json::json!({"k": "disconnected", "reason": reason,
                        "pending": eventloop.pending.len()}));
                } else {
                    shared.event(serde_json::json!({"k": "connect_failed", "reason": reason}));
                }
                tokio::time::sleep(Duration::from_millis(if was_connected { 100 } else { 500 })).await;
            }
        }
    }
}

fn describe(e: &ConnectionError) -> String {
    match e {
        ConnectionError::MqttState(s) => format!("state: {s}"),
        ConnectionError::NetworkTimeout => "network timeout".into(),
        ConnectionError::FlushTimeout => "flush timeout".into(),
        ConnectionError::Io(e) => format!("io: {e}"),
        ConnectionError::ConnectionRefused(c) => format!("refused: {c:?}"),
        ConnectionError::NotConnAck(p) => format!("not connack: {p:?}"),
        ConnectionError::RequestsDone => "requests done".into(),
        other => format!("{other}"),
    }
}

fn handle_incoming(shared: &Arc<Shared>, eventloop: &mut EventLoop, packet: Packet) {
    match packet {
        Packet::ConnAck(ack) => {
            let attempt = shared.attempt.load(Ordering::SeqCst);
            shared.conn.store(attempt, Ordering::SeqCst);
            shared.session_present.store(ack.session_present, Ordering::SeqCst);
            shared.connected.store(true, Ordering::SeqCst);
            let (_, will_qos, will_retain) = will_for(&shared.client_id, shared.boot, attempt);
            let dropped: Vec<Op> = {
                let mut inner = shared.inner.lock().unwrap();
                inner.connected = true;
                let mut dropped = Vec::new();
                if !ack.session_present {
                    // rumqttc discarded its pending list: the at-risk prefix never goes out,
                    // and every unacked publish is gone with it.
                    let n = inner.at_risk.min(inner.pending_assign.len());
                    dropped.extend(inner.pending_assign.drain(..n));
                    for (_, q) in inner.by_pkid.drain() {
                        dropped.extend(q);
                    }
                } else if inner.prev_connack {
                    // rumqttc re-sends unacknowledged publishes, but never a SUBSCRIBE or
                    // UNSUBSCRIBE: those acks will never come, so their packet ids must not
                    // be left in the map for a later op to collide with.
                    let mut stale = Vec::new();
                    inner.by_pkid.retain(|_, q| {
                        q.retain_mut(|op| {
                            if op.kind == OpKind::Publish {
                                true
                            } else {
                                stale.push(Op {
                                    id: op.id,
                                    kind: op.kind,
                                    qos: op.qos,
                                    state: op.state,
                                    sends: op.sends,
                                    done: op.done.clone(),
                                });
                                false
                            }
                        });
                        !q.is_empty()
                    });
                    dropped.extend(stale);
                }
                inner.at_risk = 0;
                inner.prev_connack = true;
                dropped
            };
            shared.event(serde_json::json!({"k": "connected", "session_present": ack.session_present,
                "attempt": attempt, "will_qos": will_qos, "will_retain": will_retain}));
            for op in dropped {
                shared.op_result(&op, "dropped", serde_json::json!({}));
                op.done.notify_one();
            }
        }
        Packet::Publish(p) => {
            let parsed = parse_payload(&p.payload);
            shared.event(serde_json::json!({"k": "delivery", "topic": p.topic, "qos": p.qos as u8,
                "retain": p.retain, "dup": p.dup, "pkid": p.pkid, "len": p.payload.len(),
                "payload": parsed}));
        }
        Packet::PubAck(a) => finish_pkid(shared, a.pkid, "acked", true),
        Packet::PubRec(r) => finish_pkid(shared, r.pkid, "accepted", false),
        Packet::PubComp(c) => finish_pkid(shared, c.pkid, "completed", true),
        Packet::SubAck(s) => {
            let codes: Vec<i32> = s
                .return_codes
                .iter()
                .map(|c| match c {
                    rumqttc::SubscribeReasonCode::Success(q) => *q as i32,
                    rumqttc::SubscribeReasonCode::Failure => 128,
                })
                .collect();
            let op = take_pkid(shared, s.pkid);
            match op {
                Some(op) => {
                    shared.op_result(&op, "suback", serde_json::json!({"codes": codes}));
                }
                None => shared.event(serde_json::json!({"k": "stray_ack", "what": "suback", "pkid": s.pkid})),
            }
        }
        Packet::UnsubAck(u) => match take_pkid(shared, u.pkid) {
            Some(op) => shared.op_result(&op, "unsuback", serde_json::json!({})),
            None => shared.event(serde_json::json!({"k": "stray_ack", "what": "unsuback", "pkid": u.pkid})),
        },
        Packet::PubRel(_) | Packet::PingResp => {}
        other => shared.event(serde_json::json!({"k": "unexpected", "packet": format!("{other:?}")})),
    }
}

/// rumqttc tracks packet ids only for publishes; a SUBSCRIBE or UNSUBSCRIBE whose ack never
/// came (rumqttd sends no UNSUBACK for an unknown filter) must not keep its id reserved, or
/// the next request rumqttc assigns that id to would be mistaken for it.
fn evict_stale(inner: &mut Inner, pkid: u16) -> Vec<Op> {
    let mut stale = Vec::new();
    if let Some(q) = inner.by_pkid.get_mut(&pkid) {
        q.retain_mut(|op| {
            if op.kind == OpKind::Publish {
                true
            } else {
                stale.push(Op { id: op.id, kind: op.kind, qos: op.qos, state: op.state, sends: op.sends, done: op.done.clone() });
                false
            }
        });
        if q.is_empty() {
            inner.by_pkid.remove(&pkid);
        }
    }
    stale
}

fn report_stale(shared: &Arc<Shared>, stale: Vec<Op>) {
    for op in stale {
        shared.op_result(&op, "lost", serde_json::json!({"reason": "packet id reused before an ack arrived"}));
    }
}

fn take_pkid(shared: &Arc<Shared>, pkid: u16) -> Option<Op> {
    let mut inner = shared.inner.lock().unwrap();
    let q = inner.by_pkid.get_mut(&pkid)?;
    let op = q.pop_front();
    if q.is_empty() {
        inner.by_pkid.remove(&pkid);
    }
    op
}

fn finish_pkid(shared: &Arc<Shared>, pkid: u16, outcome: &str, remove: bool) {
    let op = if remove {
        take_pkid(shared, pkid)
    } else {
        let inner = shared.inner.lock().unwrap();
        inner.by_pkid.get(&pkid).and_then(|q| q.front()).map(|op| Op {
            id: op.id,
            kind: op.kind,
            qos: op.qos,
            state: op.state,
            sends: op.sends,
            done: op.done.clone(),
        })
    };
    match op {
        Some(op) => shared.op_result(&op, outcome, serde_json::json!({"pkid": pkid})),
        None => shared.event(serde_json::json!({"k": "stray_ack", "what": outcome, "pkid": pkid})),
    }
}

fn handle_outgoing(shared: &Arc<Shared>, out: Outgoing) {
    match out {
        Outgoing::Publish(pkid) => {
            let mut inner = shared.inner.lock().unwrap();
            // A publish that already holds this packet id and was sent before is a
            // retransmission; one that was parked by a collision is going out for the
            // first time. Otherwise this is a fresh assignment for the head of the queue.
            let stale = if pkid == 0 { Vec::new() } else { evict_stale(&mut inner, pkid) };
            let existing = if pkid == 0 {
                None
            } else {
                inner
                    .by_pkid
                    .get_mut(&pkid)
                    .and_then(|q| q.iter_mut().find(|op| op.kind == OpKind::Publish && op.state != OpState::Queued))
            };
            if let Some(op) = existing {
                op.sends += 1;
                op.state = OpState::Sent;
                let (id, n) = (op.id, op.sends);
                drop(inner);
                report_stale(shared, stale);
                shared.event(serde_json::json!({"k": "sent", "op": id, "pkid": pkid, "n": n}));
                return;
            }
            let Some(mut op) = inner.pending_assign.pop_front() else {
                drop(inner);
                shared.event(serde_json::json!({"k": "unmatched_outgoing", "what": "publish", "pkid": pkid}));
                return;
            };
            if op.kind != OpKind::Publish {
                shared.event(serde_json::json!({"k": "mismatch", "expected": "publish", "op": op.id}));
            }
            op.sends = 1;
            op.state = OpState::Sent;
            inner.at_risk = inner.at_risk.saturating_sub(1);
            let id = op.id;
            let done = op.done.clone();
            if pkid == 0 {
                drop(inner);
                shared.event(serde_json::json!({"k": "sent", "op": id, "pkid": 0, "n": 1}));
                shared.op_result(&op, "sent", serde_json::json!({}));
            } else {
                inner.by_pkid.entry(pkid).or_default().push_back(op);
                drop(inner);
                report_stale(shared, stale);
                shared.event(serde_json::json!({"k": "sent", "op": id, "pkid": pkid, "n": 1}));
            }
            done.notify_one();
        }
        Outgoing::AwaitAck(pkid) => {
            let mut inner = shared.inner.lock().unwrap();
            let stale = evict_stale(&mut inner, pkid);
            if let Some(mut op) = inner.pending_assign.pop_front() {
                op.state = OpState::AwaitingSlot;
                inner.at_risk = inner.at_risk.saturating_sub(1);
                let done = op.done.clone();
                let id = op.id;
                inner.by_pkid.entry(pkid).or_default().push_back(op);
                drop(inner);
                report_stale(shared, stale);
                shared.event(serde_json::json!({"k": "collision", "op": id, "pkid": pkid}));
                done.notify_one();
            }
        }
        Outgoing::Subscribe(pkid) | Outgoing::Unsubscribe(pkid) => {
            let mut inner = shared.inner.lock().unwrap();
            let stale = evict_stale(&mut inner, pkid);
            if let Some(mut op) = inner.pending_assign.pop_front() {
                op.state = OpState::Sent;
                op.sends = 1;
                inner.at_risk = inner.at_risk.saturating_sub(1);
                let done = op.done.clone();
                inner.by_pkid.entry(pkid).or_default().push_back(op);
                drop(inner);
                report_stale(shared, stale);
                done.notify_one();
            } else {
                drop(inner);
                shared.event(serde_json::json!({"k": "unmatched_outgoing", "what": "subscribe", "pkid": pkid}));
            }
        }
        Outgoing::Disconnect => {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(op) = inner.pending_assign.pop_front() {
                inner.at_risk = inner.at_risk.saturating_sub(1);
                drop(inner);
                shared.op_result(&op, "disconnected", serde_json::json!({}));
                op.done.notify_one();
            }
        }
        Outgoing::PubRel(pkid) => shared.event(serde_json::json!({"k": "pubrel", "pkid": pkid})),
        Outgoing::PubAck(_) | Outgoing::PubRec(_) | Outgoing::PubComp(_) | Outgoing::PingReq | Outgoing::PingResp => {}
    }
}

async fn http_server(shared: Arc<Shared>, cmd_tx: mpsc::Sender<Cmd>, listen: &str) {
    let listener = TcpListener::bind(listen).await.expect("bind http");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let shared = shared.clone();
        let cmd_tx = cmd_tx.clone();
        tokio::spawn(async move {
            let mut buf = Vec::with_capacity(4096);
            let mut tmp = [0u8; 4096];
            let (head_end, body_len) = loop {
                let n = match tokio::time::timeout(Duration::from_secs(5), sock.read(&mut tmp)).await {
                    Ok(Ok(n)) if n > 0 => n,
                    _ => return,
                };
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    break (pos + 4, len);
                }
                if buf.len() > 1 << 20 {
                    return;
                }
            };
            while buf.len() < head_end + body_len {
                let n = match tokio::time::timeout(Duration::from_secs(5), sock.read(&mut tmp)).await {
                    Ok(Ok(n)) if n > 0 => n,
                    _ => return,
                };
                buf.extend_from_slice(&tmp[..n]);
            }
            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let line = head.lines().next().unwrap_or("");
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or("");
            let path = parts.next().unwrap_or("");
            let body = &buf[head_end..head_end + body_len];
            let (status, resp) = match (method, path) {
                ("GET", "/health") => (
                    200,
                    serde_json::to_string(&Health {
                        client: &shared.client_id,
                        boot: shared.boot,
                        conn: shared.conn.load(Ordering::SeqCst),
                        attempt: shared.attempt.load(Ordering::SeqCst),
                        connected: shared.connected.load(Ordering::SeqCst),
                        sp: shared.session_present.load(Ordering::SeqCst),
                    })
                    .unwrap(),
                ),
                ("POST", "/op") => match serde_json::from_slice::<Cmd>(body) {
                    Ok(cmd) => match cmd_tx.try_send(cmd) {
                        Ok(()) => (200, serde_json::json!({"accepted": true, "boot": shared.boot}).to_string()),
                        Err(_) => (503, serde_json::json!({"accepted": false, "busy": true, "boot": shared.boot}).to_string()),
                    },
                    Err(e) => (400, serde_json::json!({"error": e.to_string()}).to_string()),
                },
                _ => (404, "{}".to_string()),
            };
            let reason = match status { 200 => "OK", 503 => "Busy", _ => "Error" };
            let out = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
            let _ = sock.write_all(out.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}
