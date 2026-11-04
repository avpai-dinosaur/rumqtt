//! The driver's model of what the broker should contain, and the checks on it.
//!
//! Everything the clients report arrives here as JSON events (`on_client_event`), the
//! broker's and clients' forwarded log lines as `on_log_line`. Rules record what they send
//! through `issue_*` before the request goes out, so every record has an early bound for
//! when the broker could have seen it, and a late bound (the driver's time when the
//! outcome arrived) for when it must have.
use crate::topic;
use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes, assert_unreachable};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// A delivery that can't be placed in its publisher's wire order yet waits this long for the
/// matching `sent` event before it counts as out of order.
pub const ORDER_GRACE: Duration = Duration::from_secs(20);
/// A persistent session reported absent waits this long for the broker restart that explains it.
pub const SESSION_LOSS_GRACE: Duration = Duration::from_secs(30);
/// Records older than this are forgotten.
pub const RETENTION: Duration = Duration::from_secs(12 * 60);
/// Ended subscriptions and connections are kept this long (deliveries can trail them by a
/// few seconds at most).
pub const EPOCH_RETENTION: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnKey {
    pub boot: u64,
    pub conn: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpKind {
    Publish,
    Subscribe,
    Unsubscribe,
    Disconnect,
}

#[derive(Clone, Debug)]
pub struct OpMeta {
    pub client: usize,
    pub kind: OpKind,
    pub issued: Instant,
    pub qos: u8,
}

#[derive(Debug)]
pub struct Epoch {
    pub filter: String,
    pub qos: u8,
    pub session: u64,
    pub sub_op: u64,
    /// When the driver issued the SUBSCRIBE: the broker can't have registered it earlier.
    pub sub_sent: Instant,
    /// When the SUBACK arrived: the broker had registered it by then.
    pub confirmed: Option<Instant>,
    pub granted: Option<u8>,
    pub failed: bool,
    pub unsub_op: Option<u64>,
    pub unsub_sent: Option<Instant>,
    /// UNSUBACK received, session ended, or connection ended (clean sessions).
    pub ended: Option<Instant>,
}

#[derive(Debug)]
pub struct ConnRec {
    pub started: Instant,
    pub session: u64,
    pub ended: Option<Instant>,
    pub session_present: bool,
    pub will_qos: u8,
    pub will_retain: bool,
}

pub struct ClientModel {
    pub name: String,
    pub url: String,
    pub persistent: bool,
    pub boot: Option<u64>,
    pub conn: u64,
    pub connected: bool,
    /// Increments whenever the client starts a new session at the broker.
    pub session: u64,
    pub had_session: bool,
    pub conns: HashMap<ConnKey, ConnRec>,
    pub epochs: Vec<Epoch>,
    /// When a will message naming this connection was delivered to some subscriber.
    pub will_seen: HashMap<ConnKey, Instant>,
    pub last_delivery: Option<Instant>,
    pub disconnects: u64,
}

#[derive(Debug)]
pub struct Msg {
    pub op: u64,
    pub publisher: usize,
    pub pub_boot: Option<u64>,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub empty: bool,
    pub size: usize,
    pub sent: Instant,
    /// PUBACK or PUBREC received: the broker accepted ownership.
    pub acked: Option<Instant>,
    pub completed: Option<Instant>,
    pub dropped: bool,
    pub sends: u32,
    /// Live (retain=0) deliveries per (subscriber, session).
    pub deliveries: HashMap<(usize, u64), u32>,
    /// Retained (retain=1) deliveries per (subscriber, session).
    pub retained_deliveries: HashSet<(usize, u64)>,
    /// The last few deliveries: (subscriber, conn, pkid, dup, driver ms).
    pub recent: Vec<(usize, u64, u16, bool, u64)>,
}

#[derive(Default)]
pub struct WireStream {
    pub base: usize,
    pub ops: VecDeque<u64>,
}

impl WireStream {
    fn find_from(&self, op: u64, from: usize) -> Option<usize> {
        let start = from.saturating_sub(self.base).min(self.ops.len());
        self.ops
            .iter()
            .skip(start)
            .position(|&o| o == op)
            .map(|p| self.base + start + p)
    }
    fn find_last(&self, op: u64) -> Option<usize> {
        self.ops.iter().rposition(|&o| o == op).map(|p| self.base + p)
    }
    fn tail(&self, n: usize) -> Vec<u64> {
        self.ops.iter().rev().take(n).rev().copied().collect()
    }
}

#[derive(Default)]
pub struct Cursor {
    pub pos: usize,
    pub pending: VecDeque<(u64, Instant)>,
}

type StreamKey = (usize, u64, String);
type CursorKey = (usize, ConnKey, usize, u64, String);

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub bad_events: u64,
    pub dup_events: u64,
    pub lost_events: u64,
    pub synthesized_conns: u64,
    pub unknown_op: u64,
    pub http_failures: u64,
    pub deliveries: u64,
    pub order_unplaceable: u64,
    pub events: u64,
    pub log_lines: u64,
    pub pruned_msgs: u64,
    pub packets: u64,
    pub packets_received: u64,
    pub busy: u64,
}

pub struct Model {
    pub clients: Vec<ClientModel>,
    pub ops: HashMap<u64, OpMeta>,
    pub msgs: HashMap<u64, Msg>,
    /// Retained publishes (including empty clears) per topic, by op.
    pub retained_hist: HashMap<String, Vec<u64>>,
    pub wire: HashMap<StreamKey, WireStream>,
    pub cursors: HashMap<CursorKey, Cursor>,
    pub pending_cursors: HashSet<CursorKey>,
    pub broker_boot: Option<u64>,
    pub broker_boots: Vec<(u64, Instant)>,
    /// (client, time of the loss, start of the client's previous connection)
    pub session_losses: VecDeque<(usize, Instant, Option<Instant>)>,
    /// Last reported outcome per op, for the quiet-period checks.
    pub outcomes: HashMap<u64, String>,
    pub pruned_below: u64,
    pub active: bool,
    pub strict_qos: bool,
    pub start: Instant,
    pub stats: Stats,
    pub last_prune: Instant,
}

fn lt(a: Instant, b: Option<Instant>) -> bool {
    b.map_or(true, |b| a < b)
}

fn gt(a: Option<Instant>, b: Instant) -> bool {
    a.map_or(true, |a| a > b)
}

impl Msg {
    /// When the broker had stored the message: PUBACK for QoS 1, PUBCOMP for QoS 2 (the
    /// broker may defer the append until PUBREL), never for QoS 0.
    pub fn settled(&self) -> Option<Instant> {
        match self.qos {
            2 => self.completed,
            1 => self.acked,
            _ => None,
        }
    }
}

impl Model {
    pub fn new(clients: Vec<(String, String, bool)>, strict_qos: bool) -> Self {
        let now = Instant::now();
        Model {
            clients: clients
                .into_iter()
                .map(|(name, url, persistent)| ClientModel {
                    name,
                    url,
                    persistent,
                    boot: None,
                    conn: 0,
                    connected: false,
                    session: 0,
                    had_session: false,
                    conns: HashMap::new(),
                    epochs: Vec::new(),
                    will_seen: HashMap::new(),
                    last_delivery: None,
                    disconnects: 0,
                })
                .collect(),
            ops: HashMap::new(),
            msgs: HashMap::new(),
            retained_hist: HashMap::new(),
            wire: HashMap::new(),
            cursors: HashMap::new(),
            pending_cursors: HashSet::new(),
            broker_boot: None,
            broker_boots: Vec::new(),
            session_losses: VecDeque::new(),
            outcomes: HashMap::new(),
            pruned_below: 0,
            active: false,
            strict_qos,
            start: now,
            stats: Stats::default(),
            last_prune: now,
        }
    }

    pub fn ms(&self, t: Instant) -> u64 {
        t.duration_since(self.start).as_millis() as u64
    }

    fn ms_opt(&self, t: Option<Instant>) -> Option<u64> {
        t.map(|t| self.ms(t))
    }

    // ----------------------------------------------------------------------------------
    // Records made by the rules, before each request goes out.
    // ----------------------------------------------------------------------------------

    pub fn issue_publish(&mut self, op: u64, client: usize, topic: &str, qos: u8, retain: bool, size: usize, empty: bool) {
        let now = Instant::now();
        self.ops.insert(op, OpMeta { client, kind: OpKind::Publish, issued: now, qos });
        self.msgs.insert(
            op,
            Msg {
                op,
                publisher: client,
                pub_boot: None,
                topic: topic.to_string(),
                qos,
                retain,
                empty,
                size,
                sent: now,
                acked: None,
                completed: None,
                dropped: false,
                sends: 0,
                deliveries: HashMap::new(),
                retained_deliveries: HashSet::new(),
                recent: Vec::new(),
            },
        );
        if retain {
            self.retained_hist.entry(topic.to_string()).or_default().push(op);
        }
    }

    pub fn issue_subscribe(&mut self, op: u64, client: usize, filter: &str, qos: u8) {
        let now = Instant::now();
        self.ops.insert(op, OpMeta { client, kind: OpKind::Subscribe, issued: now, qos });
        let session = self.clients[client].session;
        self.clients[client].epochs.push(Epoch {
            filter: filter.to_string(),
            qos,
            session,
            sub_op: op,
            sub_sent: now,
            confirmed: None,
            granted: None,
            failed: false,
            unsub_op: None,
            unsub_sent: None,
            ended: None,
        });
    }

    pub fn issue_unsubscribe(&mut self, op: u64, client: usize, filter: &str) {
        let now = Instant::now();
        self.ops.insert(op, OpMeta { client, kind: OpKind::Unsubscribe, issued: now, qos: 0 });
        let session = self.clients[client].session;
        for e in self.clients[client].epochs.iter_mut() {
            if e.session == session && e.filter == filter && e.ended.is_none() && e.unsub_op.is_none() {
                e.unsub_op = Some(op);
                e.unsub_sent = Some(now);
            }
        }
    }

    pub fn issue_disconnect(&mut self, op: u64, client: usize) {
        self.ops.insert(op, OpMeta { client, kind: OpKind::Disconnect, issued: Instant::now(), qos: 0 });
    }

    /// Forgets an op the client program rejected before doing anything with it.
    pub fn retract(&mut self, op: u64) {
        let Some(meta) = self.ops.remove(&op) else { return };
        match meta.kind {
            OpKind::Publish => {
                if let Some(m) = self.msgs.remove(&op) {
                    if let Some(h) = self.retained_hist.get_mut(&m.topic) {
                        h.retain(|o| *o != op);
                    }
                }
            }
            OpKind::Subscribe => self.clients[meta.client].epochs.retain(|e| e.sub_op != op),
            OpKind::Unsubscribe => {
                for e in self.clients[meta.client].epochs.iter_mut() {
                    if e.unsub_op == Some(op) {
                        e.unsub_op = None;
                        e.unsub_sent = None;
                    }
                }
            }
            OpKind::Disconnect => {}
        }
    }

    /// Filters the client has confirmed, in its current session, and hasn't unsubscribed.
    pub fn live_filters(&self, client: usize) -> Vec<String> {
        let c = &self.clients[client];
        c.epochs
            .iter()
            .filter(|e| e.session == c.session && e.confirmed.is_some() && e.ended.is_none() && e.unsub_sent.is_none())
            .map(|e| e.filter.clone())
            .collect()
    }

    // ----------------------------------------------------------------------------------
    // Client events
    // ----------------------------------------------------------------------------------

    pub fn on_client_event(&mut self, idx: usize, v: &Value) {
        self.stats.events += 1;
        let kind = v["k"].as_str().unwrap_or("");
        let boot = v["boot"].as_u64().unwrap_or(0);
        let conn = v["conn"].as_u64().unwrap_or(0);
        let sp = v["sp"].as_bool().unwrap_or(false);
        self.note_boot(idx, boot);
        let key = ConnKey { boot, conn };
        match kind {
            "boot" => {}
            "connected" => {
                let attempt = v["attempt"].as_u64().unwrap_or(conn);
                self.on_connected(idx, ConnKey { boot, conn: attempt }, v["session_present"].as_bool().unwrap_or(sp));
            }
            "disconnected" => self.on_disconnected(idx, key),
            "connect_failed" => {
                if self.active {
                    assert_sometimes!(true, "a client's connection attempt fails", &json!({"client": self.clients[idx].name, "reason": v["reason"]}));
                }
            }
            "sent" => {
                self.ensure_conn(idx, key, sp);
                let op = v["op"].as_u64().unwrap_or(0);
                let n = v["n"].as_u64().unwrap_or(1) as u32;
                self.on_sent(idx, boot, op, n);
            }
            "op" => {
                self.ensure_conn(idx, key, sp);
                let op = v["op"].as_u64().unwrap_or(0);
                let outcome = v["outcome"].as_str().unwrap_or("").to_string();
                let codes: Vec<i64> = v["codes"].as_array().map(|a| a.iter().filter_map(|c| c.as_i64()).collect()).unwrap_or_default();
                self.on_op(idx, key, op, &outcome, &codes);
            }
            "delivery" => {
                self.ensure_conn(idx, key, sp);
                self.on_delivery(idx, key, v);
            }
            "collision" => {
                if self.active {
                    assert_sometimes!(true, "a client parks a publish on a packet id collision", &json!({"client": self.clients[idx].name}));
                }
            }
            "pubrel" => {}
            "stray_ack" => {
                assert_unreachable!("a client receives an acknowledgement it has no outstanding request for", &json!({"client": self.clients[idx].name, "what": v["what"], "pkid": v["pkid"]}));
            }
            "unmatched_outgoing" | "mismatch" | "stuck" => {
                assert_unreachable!("the client program's packet-id bookkeeping loses track of a request", &json!({"client": self.clients[idx].name, "event": v}));
            }
            "unexpected" => {
                assert_unreachable!("a client receives a packet type the broker must not send", &json!({"client": self.clients[idx].name, "packet": v["packet"]}));
            }
            _ => self.stats.bad_events += 1,
        }
    }

    fn note_boot(&mut self, idx: usize, boot: u64) {
        let now = Instant::now();
        let c = &mut self.clients[idx];
        if c.boot == Some(boot) {
            return;
        }
        let restarted = c.boot.is_some();
        for (k, r) in c.conns.iter_mut() {
            if k.boot != boot && r.ended.is_none() {
                r.ended = Some(now);
            }
        }
        c.boot = Some(boot);
        c.connected = false;
        if !c.persistent {
            let session = c.session;
            Self::end_epochs(c, session, now);
        }
        if restarted && self.active {
            assert_reachable!("the driver notices a client restart", &json!({"client": self.clients[idx].name}));
        }
    }

    fn end_epochs(c: &mut ClientModel, session: u64, now: Instant) {
        for e in c.epochs.iter_mut() {
            if e.session == session && e.ended.is_none() {
                e.ended = Some(now);
            }
        }
    }

    fn ensure_conn(&mut self, idx: usize, key: ConnKey, sp: bool) {
        if key.conn == 0 || self.clients[idx].conns.contains_key(&key) {
            return;
        }
        self.stats.synthesized_conns += 1;
        self.on_connected(idx, key, sp);
    }

    fn on_connected(&mut self, idx: usize, key: ConnKey, sp: bool) {
        let now = Instant::now();
        let active = self.active;
        let c = &mut self.clients[idx];
        if c.conns.contains_key(&key) {
            return;
        }
        let prev_started = c.conns.values().map(|r| r.started).max();
        for (k, r) in c.conns.iter_mut() {
            if *k != key && r.ended.is_none() {
                r.ended = Some(now);
            }
        }
        let name = c.name.clone();
        if c.persistent {
            if sp {
                if active {
                    assert_sometimes!(true, "a persistent session is resumed on reconnect (MQTT-3.1.2-4)", &json!({"client": name, "conn": key.conn}));
                }
            } else if c.had_session {
                let session = c.session;
                Self::end_epochs(c, session, now);
                c.session += 1;
                self.session_losses.push_back((idx, now, prev_started));
                if active {
                    assert_sometimes!(true, "a persistent client finds its session gone on reconnect", &json!({"client": name, "conn": key.conn}));
                }
            }
            c.had_session = true;
        } else {
            assert_always!(!sp, "a clean-session connection never reports session present (MQTT-3.2.2-1)", &json!({"client": name, "conn": key.conn}));
            let session = c.session;
            Self::end_epochs(c, session, now);
            c.session += 1;
        }
        let c = &mut self.clients[idx];
        c.conns.insert(
            key,
            ConnRec {
                started: now,
                session: c.session,
                ended: None,
                session_present: sp,
                will_qos: (key.conn % 3) as u8,
                will_retain: key.conn % 2 == 1,
            },
        );
        c.conn = key.conn;
        c.connected = true;
        if active && key.conn > 1 {
            assert_reachable!("a client reconnects to the broker", &json!({"client": name, "conn": key.conn}));
        }
    }

    fn on_disconnected(&mut self, idx: usize, key: ConnKey) {
        let now = Instant::now();
        let c = &mut self.clients[idx];
        c.disconnects += 1;
        c.connected = false;
        let session = c.conns.get(&key).map(|r| r.session);
        if let Some(r) = c.conns.get_mut(&key) {
            if r.ended.is_none() {
                r.ended = Some(now);
            }
        }
        if !c.persistent {
            if let Some(session) = session {
                Self::end_epochs(c, session, now);
            }
        }
        if self.active {
            assert_reachable!("a client loses its connection to the broker", &json!({"client": self.clients[idx].name}));
        }
    }

    fn on_sent(&mut self, idx: usize, boot: u64, op: u64, n: u32) {
        if op < self.pruned_below {
            return;
        }
        let Some(msg) = self.msgs.get_mut(&op) else {
            self.stats.unknown_op += 1;
            return;
        };
        if msg.publisher != idx {
            return;
        }
        msg.sends = msg.sends.max(n);
        if msg.pub_boot.is_none() {
            msg.pub_boot = Some(boot);
        }
        let topic = msg.topic.clone();
        if n > 1 && self.active {
            assert_sometimes!(true, "a client retransmits an unacknowledged publish after reconnecting (MQTT-4.4.0-1)", &json!({"client": self.clients[idx].name, "op": op, "n": n}));
        }
        let skey: StreamKey = (idx, boot, topic.clone());
        self.wire.entry(skey.clone()).or_default().ops.push_back(op);
        // Deliveries that were waiting for this send may be placeable now.
        let waiting: Vec<CursorKey> = self
            .pending_cursors
            .iter()
            .filter(|k| k.2 == idx && k.3 == boot && k.4 == topic)
            .cloned()
            .collect();
        for ckey in waiting {
            self.resolve_cursor(&ckey);
        }
    }

    fn on_op(&mut self, idx: usize, key: ConnKey, op: u64, outcome: &str, codes: &[i64]) {
        let now = Instant::now();
        if op < self.pruned_below {
            return;
        }
        let Some(meta) = self.ops.get(&op).cloned() else {
            self.stats.unknown_op += 1;
            return;
        };
        let broker_response = matches!(outcome, "acked" | "accepted" | "completed" | "suback" | "unsuback");
        self.outcomes.insert(op, outcome.to_string());
        if broker_response {
            if let Some(&seen) = self.clients[idx].will_seen.get(&key) {
                assert_always!(
                    meta.issued <= seen,
                    "the broker never publishes the will of a connection that is still alive (MQTT-3.1.2-8)",
                    &json!({"client": self.clients[idx].name, "conn": key.conn, "boot": key.boot, "op": op,
                            "outcome": outcome, "issued_ms": self.ms(meta.issued), "will_seen_ms": self.ms(seen)})
                );
            }
        }
        let active = self.active;
        match meta.kind {
            OpKind::Publish => {
                let Some(msg) = self.msgs.get_mut(&op) else { return };
                match outcome {
                    "acked" | "accepted" => {
                        if msg.acked.is_none() {
                            msg.acked = Some(now);
                            let latency = now.duration_since(msg.sent);
                            if active {
                                assert_sometimes!(latency > Duration::from_secs(5), "a publish is acknowledged more than 5 s after it was sent", &json!({"op": op, "latency_ms": latency.as_millis() as u64}));
                            }
                            if msg.qos == 1 {
                                assert_reachable!("a QoS 1 publish is acknowledged", &json!({}));
                            } else if msg.qos == 2 {
                                assert_reachable!("a QoS 2 publish is accepted with PUBREC", &json!({}));
                            }
                        }
                    }
                    "completed" => {
                        if msg.acked.is_none() {
                            msg.acked = Some(now);
                        }
                        msg.completed = Some(now);
                        assert_reachable!("a QoS 2 publish completes with PUBCOMP", &json!({}));
                    }
                    "sent" => {
                        assert_reachable!("a QoS 0 publish is sent", &json!({}));
                    }
                    "dropped" | "invalid" => {
                        if msg.acked.is_none() {
                            msg.dropped = true;
                            if msg.qos > 0 && active && outcome == "dropped" {
                                assert_sometimes!(true, "an unacknowledged publish is discarded because the session was not resumed", &json!({"client": self.clients[idx].name, "op": op}));
                            }
                        }
                    }
                    _ => {}
                }
            }
            OpKind::Subscribe => {
                if outcome == "suback" {
                    let name = self.clients[idx].name.clone();
                    let c = &mut self.clients[idx];
                    // A SUBSCRIBE queued at the client across a reconnect takes effect on the
                    // connection that acknowledged it, so that connection's session owns it.
                    let ack_session = c.conns.get(&key).map(|r| r.session);
                    let Some(e) = c.epochs.iter_mut().find(|e| e.sub_op == op) else { return };
                    if let Some(s) = ack_session {
                        e.session = s;
                    }
                    assert_always!(codes.len() == 1, "a SUBACK carries one return code per requested filter (MQTT-3.8.4-5)", &json!({"client": name, "filter": e.filter, "codes": codes}));
                    let code = codes.first().copied().unwrap_or(128);
                    if code >= 128 {
                        e.failed = true;
                        e.ended = Some(now);
                        assert_unreachable!("the broker refuses a valid subscription", &json!({"client": name, "filter": e.filter, "code": code}));
                    } else {
                        assert_always!(code <= e.qos as i64, "a SUBACK grants at most the requested QoS (MQTT-3.8.4-5)", &json!({"client": name, "filter": e.filter, "requested": e.qos, "granted": code}));
                        e.confirmed = Some(now);
                        e.granted = Some(code as u8);
                        assert_reachable!("a subscription is granted", &json!({}));
                    }
                }
            }
            OpKind::Unsubscribe => {
                if outcome == "unsuback" {
                    let c = &mut self.clients[idx];
                    for e in c.epochs.iter_mut() {
                        if e.unsub_op == Some(op) && e.ended.is_none() {
                            e.ended = Some(now);
                        }
                    }
                    assert_reachable!("an unsubscribe is acknowledged", &json!({}));
                }
            }
            OpKind::Disconnect => {
                if outcome == "disconnected" {
                    assert_reachable!("a client disconnects gracefully with DISCONNECT", &json!({}));
                }
            }
        }
    }

    fn on_delivery(&mut self, idx: usize, key: ConnKey, v: &Value) {
        let now = Instant::now();
        self.stats.deliveries += 1;
        self.clients[idx].last_delivery = Some(now);
        let topic = v["topic"].as_str().unwrap_or("").to_string();
        let qos = v["qos"].as_u64().unwrap_or(0) as u8;
        let retain = v["retain"].as_bool().unwrap_or(false);
        let payload = &v["payload"];
        let fmt = payload["fmt"].as_str().unwrap_or("");
        let name = self.clients[idx].name.clone();
        let Some(conn) = self.clients[idx].conns.get(&key) else { return };
        let session = conn.session;
        let conn_started = conn.started;
        let conn_ended = conn.ended;
        let conn_sp = conn.session_present;

        // The subscriptions this delivery can belong to: the connection's session, matching.
        let matching: Vec<usize> = self.clients[idx]
            .epochs
            .iter()
            .enumerate()
            .filter(|(_, e)| e.session == session && !e.failed && topic::matches(&e.filter, &topic))
            .map(|(i, _)| i)
            .collect();
        let any_wildcard = matching.iter().any(|&i| topic::is_wildcard(&self.clients[idx].epochs[i].filter));
        // Subscriptions that were live at some point during this connection.
        let overlapping: usize = matching
            .iter()
            .filter(|&&i| {
                let e = &self.clients[idx].epochs[i];
                lt(e.sub_sent, conn_ended) && gt(e.ended, conn_started)
            })
            .count();

        match fmt {
            "will" => {
                let raw = payload["raw"].as_str().unwrap_or("");
                let parts: Vec<&str> = raw.split('|').collect();
                let (pub_name, pboot, attempt) = (
                    parts.get(1).copied().unwrap_or(""),
                    parts.get(2).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
                    parts.get(3).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
                );
                let Some(pidx) = self.clients.iter().position(|c| c.name == pub_name) else {
                    assert_always!(false, "a delivered will message names a client of the workload", &json!({"subscriber": name, "raw": raw}));
                    return;
                };
                let wkey = ConnKey { boot: pboot, conn: attempt };
                let sent = self.clients[pidx].conns.get(&wkey).map(|r| r.started).unwrap_or(now);
                self.clients[pidx].will_seen.entry(wkey).or_insert(now);
                let eligible = matching.iter().any(|&i| lt(sent, self.clients[idx].epochs[i].ended));
                assert_always!(eligible, "a will message is delivered only to a matching live subscription", &json!({"subscriber": name, "topic": topic, "will_of": pub_name, "conn": attempt}));
                if self.active {
                    assert_sometimes!(true, "a will message is delivered after a client's connection is lost (MQTT-3.1.2-8)", &json!({"will_of": pub_name, "subscriber": name}));
                    assert_sometimes!(retain, "a retained will message is delivered to a new subscription (MQTT-3.1.2-17)", &json!({"will_of": pub_name}));
                }
            }
            "empty" => {
                if self.active {
                    assert_sometimes!(true, "an empty-payload message is delivered", &json!({"topic": topic}));
                }
            }
            "msg" => {
                let intact = payload["intact"].as_bool().unwrap_or(false);
                assert_always!(intact, "a delivered payload is intact (no corruption)", &json!({"subscriber": name, "topic": topic, "payload": payload}));
                let Some(op) = payload["op"].as_u64() else { return };
                if op < self.pruned_below {
                    return;
                }
                if !self.msgs.contains_key(&op) {
                    assert_always!(false, "a delivered message was published by the workload (no phantom messages)", &json!({"subscriber": name, "topic": topic, "op": op}));
                    return;
                }
                let (m_topic, m_qos, m_retain, m_empty, m_sent, m_acked, m_pub, m_pub_boot, m_size) = {
                    let m = &self.msgs[&op];
                    (m.topic.clone(), m.qos, m.retain, m.empty, m.sent, m.settled(), m.publisher, m.pub_boot, m.size)
                };
                assert_always!(m_topic == topic, "a message is delivered on the topic it was published to", &json!({"subscriber": name, "topic": topic, "published_on": m_topic, "op": op}));
                let pub_name = self.clients[m_pub].name.clone();

                if retain {
                    assert_always!(m_retain && !m_empty, "a message delivered with the retain flag was published as a retained message (MQTT-3.3.1-8/9)", &json!({"subscriber": name, "topic": topic, "op": op}));
                    let plausible = matching.iter().any(|&i| self.retained_plausible(op, idx, i));
                    if plausible {
                        assert_always!(true, "a retained message delivered to a new subscription is the topic's latest retained message (MQTT-3.3.1-6)", &json!({}));
                    } else {
                        assert_always!(false, "a retained message delivered to a new subscription is the topic's latest retained message (MQTT-3.3.1-6)",
                            &json!({"subscriber": name, "topic": topic, "op": op, "sent_ms": self.ms(m_sent), "settled_ms": self.ms_opt(m_acked), "session": session,
                                    "epochs": self.describe_epochs(idx, session),
                                    "history": self.retained_hist.get(&topic).map(|h| h.iter().rev().take(5).map(|o| self.describe_msg(*o)).collect::<Vec<_>>())}));
                    }
                    self.msgs.get_mut(&op).unwrap().retained_deliveries.insert((idx, session));
                    if self.active {
                        assert_sometimes!(true, "a retained message is delivered to a new subscription (MQTT-3.3.1-6)", &json!({"topic": topic}));
                    }
                    return;
                }

                let eligible_epochs: Vec<usize> = matching
                    .iter()
                    .copied()
                    .filter(|&i| {
                        let e = &self.clients[idx].epochs[i];
                        lt(m_sent, e.ended) && gt(m_acked, e.sub_sent)
                    })
                    .collect();
                let eligible = !eligible_epochs.is_empty();
                if eligible {
                    assert_always!(true, "a message is delivered only to a matching subscription that was live when it was published (MQTT-3.3.2-3, MQTT-3.10.4-2)", &json!({}));
                } else {
                    assert_always!(false, "a message is delivered only to a matching subscription that was live when it was published (MQTT-3.3.2-3, MQTT-3.10.4-2)",
                        &json!({"subscriber": name, "topic": topic, "op": op, "publisher": pub_name, "sent_ms": self.ms(m_sent), "settled_ms": self.ms_opt(m_acked),
                                "session": session, "epochs": self.describe_epochs(idx, session)}));
                }

                let pkid = v["pkid"].as_u64().unwrap_or(0) as u16;
                let dup = v["dup"].as_bool().unwrap_or(false);
                let now_ms = self.ms(now);
                let count = {
                    let m = self.msgs.get_mut(&op).unwrap();
                    let c = m.deliveries.entry((idx, session)).or_insert(0);
                    *c += 1;
                    if m.recent.len() >= 8 {
                        m.recent.remove(0);
                    }
                    m.recent.push((idx, key.conn, pkid, dup, now_ms));
                    *c
                };
                // The QoS and copy bounds come from the subscriptions this delivery can belong to.
                let max_granted = eligible_epochs
                    .iter()
                    .map(|&i| {
                        let e = &self.clients[idx].epochs[i];
                        e.granted.unwrap_or(e.qos)
                    })
                    .max();
                let copies = eligible_epochs.len().max(1) as u32;
                let eff = match max_granted {
                    Some(mg) if self.strict_qos => m_qos.min(mg),
                    Some(mg) => mg,
                    None => m_qos,
                };
                if let Some(mg) = max_granted {
                    if qos <= mg {
                        assert_always!(true, "a forwarded message's QoS never exceeds the subscription's granted QoS (MQTT-3.8.4-6)", &json!({}));
                    } else {
                        assert_always!(false, "a forwarded message's QoS never exceeds the subscription's granted QoS (MQTT-3.8.4-6)",
                            &json!({"subscriber": name, "topic": topic, "op": op, "qos": qos, "granted": mg, "epochs": self.describe_epochs(idx, session)}));
                    }
                }
                if self.strict_qos {
                    assert_always!(qos <= m_qos, "a forwarded message's QoS never exceeds the QoS it was published with (MQTT-3.8.4-6)",
                        &json!({"subscriber": name, "topic": topic, "op": op, "published_qos": m_qos, "delivered_qos": qos}));
                }
                let recent_details = |me: &Model| -> Vec<Value> {
                    me.msgs[&op].recent.iter().map(|(s, c, p, d, t)| json!({"subscriber": me.clients[*s].name, "conn": c, "pkid": p, "dup": d, "ms": t})).collect()
                };
                match eff {
                    2 => {
                        if count <= copies {
                            assert_always!(true, "a QoS 2 message is delivered exactly once per matching subscription (MQTT-4.3.3)", &json!({}));
                        } else {
                            assert_always!(false, "a QoS 2 message is delivered exactly once per matching subscription (MQTT-4.3.3)",
                                &json!({"subscriber": name, "topic": topic, "op": op, "publisher": pub_name, "count": count, "allowed": copies,
                                    "session": session, "published_qos": m_qos, "recent": recent_details(self), "epochs": self.describe_epochs(idx, session)}));
                        }
                    }
                    0 => {
                        if count <= copies {
                            assert_always!(true, "a QoS 0 message is delivered at most once per matching subscription (MQTT-4.3.1)", &json!({}));
                        } else {
                            assert_always!(false, "a QoS 0 message is delivered at most once per matching subscription (MQTT-4.3.1)",
                                &json!({"subscriber": name, "topic": topic, "op": op, "publisher": pub_name, "count": count, "allowed": copies,
                                    "session": session, "published_qos": m_qos, "recent": recent_details(self), "epochs": self.describe_epochs(idx, session)}));
                        }
                    }
                    _ => {}
                }
                match eff {
                    2 | 0 => {}
                    _ => {
                        if self.active {
                            assert_sometimes!(count > copies, "a QoS 1 message is delivered more than once to the same subscriber", &json!({"topic": topic, "op": op, "count": count}));
                        }
                    }
                }

                if self.active {
                    assert_sometimes!(any_wildcard, "a wildcard subscription receives a message", &json!({"topic": topic}));
                    assert_sometimes!(overlapping > 1, "a subscriber with overlapping subscriptions receives a message", &json!({"topic": topic, "overlapping": overlapping}));
                    assert_sometimes!(conn_sp && m_sent < conn_started, "a message published while a persistent client was disconnected is delivered after it reconnects (MQTT-3.1.2-5)", &json!({"subscriber": name, "topic": topic}));
                    assert_sometimes!(m_size > 8192, "a large message is delivered intact", &json!({"size": m_size}));
                    assert_sometimes!(qos == 2, "a message is delivered at QoS 2", &json!({}));
                }

                if overlapping == 1 {
                    if let Some(pboot) = m_pub_boot {
                        let ckey: CursorKey = (idx, key, m_pub, pboot, topic.clone());
                        let cursor = self.cursors.entry(ckey.clone()).or_default();
                        cursor.pending.push_back((op, now));
                        self.pending_cursors.insert(ckey.clone());
                        self.resolve_cursor(&ckey);
                    } else {
                        self.stats.order_unplaceable += 1;
                    }
                }
            }
            _ => {
                assert_always!(false, "a delivered payload is one the workload published", &json!({"subscriber": name, "topic": topic, "payload": payload}));
            }
        }
    }

    /// Places the cursor's pending deliveries in the publisher's wire order, in arrival
    /// order, as far as the sends seen so far allow.
    fn resolve_cursor(&mut self, ckey: &CursorKey) {
        let skey: StreamKey = (ckey.2, ckey.3, ckey.4.clone());
        loop {
            let Some(cursor) = self.cursors.get_mut(ckey) else { return };
            let Some(&(op, _)) = cursor.pending.front() else {
                self.pending_cursors.remove(ckey);
                return;
            };
            let Some(stream) = self.wire.get(&skey) else { return };
            match stream.find_from(op, cursor.pos) {
                Some(i) => {
                    cursor.pos = i + 1;
                    cursor.pending.pop_front();
                }
                None => {
                    if stream.find_last(op).is_none() {
                        // No send seen for it at all: nothing to compare against. Skip it
                        // rather than guess.
                        self.stats.order_unplaceable += 1;
                        cursor.pending.pop_front();
                        continue;
                    }
                    // Seen only before the cursor: either out of order, or a retransmission
                    // whose `sent` event hasn't arrived. Wait for the grace period.
                    return;
                }
            }
        }
    }

    fn retained_plausible(&self, op: u64, idx: usize, epoch: usize) -> bool {
        // rumqttd reads the retained messages when the subscription is first consumed, which
        // can be after the SUBACK, so the only sound bound is "not superseded by a retained
        // publish the broker had stored before the SUBSCRIBE was sent".
        let e = &self.clients[idx].epochs[epoch];
        let m = &self.msgs[&op];
        let Some(msettled) = m.settled() else { return true };
        let Some(hist) = self.retained_hist.get(&m.topic) else { return true };
        !hist.iter().any(|&o2| {
            if o2 == op {
                return false;
            }
            let Some(m2) = self.msgs.get(&o2) else { return false };
            m2.settled().map_or(false, |a| a < e.sub_sent) && m2.sent > msettled
        })
    }

    fn describe_msg(&self, op: u64) -> Value {
        match self.msgs.get(&op) {
            Some(m) => json!({"op": op, "qos": m.qos, "retain": m.retain, "empty": m.empty, "sent_ms": self.ms(m.sent), "settled_ms": self.ms_opt(m.settled())}),
            None => json!({"op": op}),
        }
    }

    pub fn describe_epochs(&self, idx: usize, session: u64) -> Vec<Value> {
        self.clients[idx]
            .epochs
            .iter()
            .filter(|e| e.session == session)
            .map(|e| json!({"filter": e.filter, "qos": e.qos, "sub_sent_ms": self.ms(e.sub_sent), "confirmed_ms": self.ms_opt(e.confirmed),
                            "unsub_sent_ms": self.ms_opt(e.unsub_sent), "ended_ms": self.ms_opt(e.ended)}))
            .collect()
    }

    // ----------------------------------------------------------------------------------
    // Forwarded logs
    // ----------------------------------------------------------------------------------

    pub fn on_log_line(&mut self, node: &str, boot: u64, _seq: u64, line: &str) {
        self.stats.log_lines += 1;
        let now = Instant::now();
        if node.starts_with("broker") {
            if self.broker_boot != Some(boot) {
                let restart = self.broker_boot.is_some();
                self.broker_boot = Some(boot);
                self.broker_boots.push((boot, now));
                if restart && self.active {
                    assert_sometimes!(true, "the broker restarts", &json!({"boot": boot}));
                }
            }
            if line.contains("panicked at") {
                assert_unreachable!("the broker panics", &json!({"line": line}));
                return;
            }
            let Ok(v) = serde_json::from_str::<Value>(line) else { return };
            let msg = v["fields"]["message"].as_str().unwrap_or("");
            let client_id = v["span"]["client_id"].as_str().or_else(|| v["spans"].as_array().and_then(|s| s.iter().find_map(|x| x["client_id"].as_str()))).unwrap_or("");
            if msg == "Client connection registered" {
                assert_reachable!("the broker logs a client connection", &json!({}));
            } else if msg.starts_with("Duplicate client_id") {
                if self.active {
                    assert_sometimes!(true, "the broker replaces a connection with a new one using the same client id (MQTT-3.1.4-2)", &json!({"client_id": client_id}));
                }
            } else if msg.contains("Unsolicited/ooo ack") || msg.starts_with("ack received for pkid") {
                assert_unreachable!("the broker receives an unsolicited or out-of-order acknowledgement from a client", &json!({"client_id": client_id, "message": msg, "fields": v["fields"]}));
            } else if msg == "Router done!" {
                assert_unreachable!("the broker's router thread exits", &json!({"fields": v["fields"]}));
            } else if msg.starts_with("Failed to append to commitlog") {
                assert_unreachable!("the broker rejects a publish from the workload", &json!({"client_id": client_id, "fields": v["fields"]}));
            } else if msg == "no space for new connection" {
                assert_unreachable!("the broker runs out of connection slots", &json!({}));
            } else if msg == "Disconnecting connection" {
                assert_reachable!("the broker logs a client disconnection", &json!({}));
            } else if msg.starts_with("Adding subscription on topic") {
                assert_reachable!("the broker logs a subscription", &json!({}));
            }
        } else if line.contains("panicked at") {
            assert_unreachable!("a client program panics", &json!({"node": node, "line": line}));
        }
    }

    // ----------------------------------------------------------------------------------
    // Periodic work
    // ----------------------------------------------------------------------------------

    /// Deferred checks and pruning. Cheap; runs after every round.
    pub fn sweep(&mut self) {
        let now = Instant::now();
        // Out-of-order deliveries that the grace period didn't explain.
        let stale: Vec<CursorKey> = self
            .pending_cursors
            .iter()
            .filter(|k| self.cursors.get(k).and_then(|c| c.pending.front()).map_or(false, |(_, t)| now.duration_since(*t) > ORDER_GRACE))
            .cloned()
            .collect();
        for ckey in stale {
            let skey: StreamKey = (ckey.2, ckey.3, ckey.4.clone());
            let Some(cursor) = self.cursors.get_mut(&ckey) else { continue };
            let Some((op, _)) = cursor.pending.pop_front() else { continue };
            let (pos, last, tail) = match self.wire.get(&skey) {
                Some(s) => (cursor.pos, s.find_last(op), s.tail(12)),
                None => (cursor.pos, None, vec![]),
            };
            if let Some(last) = last {
                cursor.pos = last + 1;
            }
            assert_always!(false, "a subscriber receives one publisher's messages to a topic in the order they were sent (MQTT-4.6.0-6)",
                &json!({"subscriber": self.clients[ckey.0].name, "conn": ckey.1.conn, "publisher": self.clients[ckey.2].name, "topic": ckey.4,
                        "op": op, "cursor": pos, "last_seen_at": last, "wire_tail": tail}));
            self.resolve_cursor(&ckey);
        }
        // Session losses, once a broker restart has had time to show up in the logs.
        while let Some(&(idx, t, prev)) = self.session_losses.front() {
            if now.duration_since(t) < SESSION_LOSS_GRACE {
                break;
            }
            self.session_losses.pop_front();
            let explained = self.broker_boots.iter().any(|(_, learned)| {
                prev.map_or(true, |p| *learned > p) && *learned < t + SESSION_LOSS_GRACE
            });
            assert_always!(explained, "a persistent session survives as long as the broker stays up (MQTT-3.1.2-4)",
                &json!({"client": self.clients[idx].name, "lost_ms": self.ms(t), "prev_conn_ms": self.ms_opt(prev),
                        "broker_boots_ms": self.broker_boots.iter().map(|(_, l)| self.ms(*l)).collect::<Vec<_>>()}));
            if explained {
                assert_sometimes!(true, "a persistent session is lost because the broker restarted", &json!({"client": self.clients[idx].name}));
            }
        }
        if now.duration_since(self.last_prune) > Duration::from_secs(30) {
            self.last_prune = now;
            self.prune(now);
        }
    }

    fn prune(&mut self, now: Instant) {
        let before = self.msgs.len();
        self.msgs.retain(|_, m| now.duration_since(m.sent) < RETENTION);
        self.stats.pruned_msgs += (before - self.msgs.len()) as u64;
        self.pruned_below = self.msgs.keys().copied().min().unwrap_or(u64::MAX).min(self.ops.keys().copied().max().map_or(0, |m| m + 1));
        for h in self.retained_hist.values_mut() {
            h.retain(|o| self.msgs.contains_key(o));
        }
        self.retained_hist.retain(|_, h| !h.is_empty());
        self.ops.retain(|_, o| now.duration_since(o.issued) < RETENTION);
        self.outcomes.retain(|o, _| self.ops.contains_key(o));
        for s in self.wire.values_mut() {
            while let Some(&front) = s.ops.front() {
                if self.msgs.contains_key(&front) {
                    break;
                }
                s.ops.pop_front();
                s.base += 1;
            }
        }
        for c in self.clients.iter_mut() {
            c.conns.retain(|_, r| r.ended.map_or(true, |e| now.duration_since(e) < EPOCH_RETENTION));
            c.epochs.retain(|e| e.ended.map_or(true, |t| now.duration_since(t) < EPOCH_RETENTION));
            c.will_seen.retain(|_, t| now.duration_since(*t) < RETENTION);
        }
        let ended: HashSet<ConnKey> = self
            .clients
            .iter()
            .flat_map(|c| c.conns.iter().filter(|(_, r)| r.ended.map_or(false, |e| now.duration_since(e) > Duration::from_secs(120))).map(|(k, _)| *k))
            .collect();
        self.cursors.retain(|k, _| !ended.contains(&k.1) && self.clients[k.0].conns.contains_key(&k.1));
        self.pending_cursors.retain(|k| self.cursors.contains_key(k));
    }

    pub fn summary(&self) -> Value {
        json!({
            "events": self.stats.events,
            "packets": self.stats.packets,
            "packets_received": self.stats.packets_received,
            "bad_events": self.stats.bad_events,
            "dup_events": self.stats.dup_events,
            "deliveries": self.stats.deliveries,
            "msgs": self.msgs.len(),
            "lost_events": self.stats.lost_events,
            "synthesized_conns": self.stats.synthesized_conns,
            "order_unplaceable": self.stats.order_unplaceable,
            "http_failures": self.stats.http_failures,
            "busy": self.stats.busy,
            "clients": self.clients.iter().map(|c| json!({"name": c.name, "connected": c.connected, "conn": c.conn, "session": c.session, "disconnects": c.disconnects, "live_filters": self.live_filters_of(c)})).collect::<Vec<_>>(),
        })
    }

    fn live_filters_of(&self, c: &ClientModel) -> Vec<String> {
        c.epochs
            .iter()
            .filter(|e| e.session == c.session && e.confirmed.is_some() && e.ended.is_none() && e.unsub_sent.is_none())
            .map(|e| e.filter.clone())
            .collect()
    }
}
