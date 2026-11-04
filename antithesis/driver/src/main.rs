//! The Antithesis driver for rumqttd: a hegel concurrent state machine whose rules send
//! MQTT operations to the client containers, with a model (`model.rs`) that checks every
//! outcome and delivery against the MQTT 3.1.1 guarantees.
mod model;
mod net;
mod quiet;
mod topic;

use antithesis_sdk::{antithesis_init, assert_reachable, assert_sometimes};
use hegel::generators as gs;
use hegel::TestCase;
use model::Model;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Created by the driver container's `first_` test command when fuzzing starts.
const WORKLOAD_ACTIVE: &str = "/workload-active";

pub const TOPICS: &[&str] = &["t/a/x", "t/a/y", "t/b/x", "t/b/y", "t/c"];
pub const FILTERS: &[&str] = &[
    "t/a/x", "t/a/y", "t/b/x", "t/b/y", "t/c", "t/a/+", "t/+/x", "t/#", "#", "will/#",
];

pub struct ClientInfo {
    pub name: String,
    pub url: String,
    pub persistent: bool,
}

pub struct Driver {
    pub model: Arc<Mutex<Model>>,
    pub agent: ureq::Agent,
    pub next_op: AtomicU64,
    pub clients: Vec<ClientInfo>,
    pub quiet_p: f64,
    pub rounds: AtomicU64,
    pub quiet_count: AtomicU64,
    pub broker_host: String,
    pub broker_port: u16,
    pub started: Instant,
    /// Highest QoS the workload subscribes with (MAX_SUB_QOS, default 2).
    pub max_sub_qos: u8,
    /// Highest QoS the workload publishes with (MAX_PUB_QOS, default 2).
    pub max_pub_qos: u8,
    /// Invocations per rule: publish, burst, subscribe, unsubscribe, disconnect.
    pub rule_counts: [AtomicU64; 5],
}

pub fn log(msg: &str) {
    println!("[driver] {msg}");
}

impl Driver {
    /// Sends an op to a client. A client whose queue is full rejects it; the op is then
    /// forgotten and the worker backs off a little.
    fn issue(&self, ci: usize, op: u64, body: Value) {
        match net::post_op(&self.agent, &self.clients[ci].url, &body) {
            Ok(_) => {}
            Err(net::PostError::Busy) => {
                let mut m = self.model.lock().unwrap();
                m.retract(op);
                m.stats.busy += 1;
                if m.active {
                    assert_sometimes!(true, "a client program rejects an operation because its queue is full", &json!({"client": self.clients[ci].name}));
                }
                drop(m);
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(net::PostError::Other(e)) => {
                let mut m = self.model.lock().unwrap();
                m.stats.http_failures += 1;
                if m.active {
                    assert_sometimes!(true, "a client program is unreachable when the driver sends it an operation", &json!({"client": self.clients[ci].name, "error": e}));
                }
            }
        }
    }

    pub fn publish_one(&self, ci: usize, topic: &str, qos: u8, retain: bool, size: usize, empty: bool) -> u64 {
        let op = self.next_op.fetch_add(1, Ordering::SeqCst);
        self.model.lock().unwrap().issue_publish(op, ci, topic, qos, retain, size, empty);
        self.issue(ci, op, json!({"kind": "publish", "id": op, "topic": topic, "qos": qos, "retain": retain, "size": size, "empty": empty}));
        op
    }

    pub fn subscribe_one(&self, ci: usize, filter: &str, qos: u8) -> u64 {
        let op = self.next_op.fetch_add(1, Ordering::SeqCst);
        self.model.lock().unwrap().issue_subscribe(op, ci, filter, qos);
        self.issue(ci, op, json!({"kind": "subscribe", "id": op, "filter": filter, "qos": qos}));
        op
    }

    pub fn unsubscribe_one(&self, ci: usize, filter: &str) -> u64 {
        let op = self.next_op.fetch_add(1, Ordering::SeqCst);
        self.model.lock().unwrap().issue_unsubscribe(op, ci, filter);
        self.issue(ci, op, json!({"kind": "unsubscribe", "id": op, "filter": filter}));
        op
    }

    pub fn disconnect_one(&self, ci: usize) -> u64 {
        let op = self.next_op.fetch_add(1, Ordering::SeqCst);
        self.model.lock().unwrap().issue_disconnect(op, ci);
        self.issue(ci, op, json!({"kind": "disconnect", "id": op}));
        op
    }

    fn draw_client(&self, tc: &TestCase) -> usize {
        tc.draw(gs::integers::<usize>().min_value(0).max_value(self.clients.len() - 1))
    }

    fn draw_qos(&self, tc: &TestCase) -> u8 {
        let r: u32 = tc.draw(gs::integers::<u32>().min_value(0).max_value(99));
        let qos = if r < 30 {
            0
        } else if r < 70 {
            1
        } else {
            2
        };
        qos.min(self.max_pub_qos)
    }

    fn draw_size(tc: &TestCase) -> usize {
        let class: u32 = tc.draw(gs::integers::<u32>().min_value(0).max_value(99));
        let (lo, hi) = if class < 60 {
            (0, 64)
        } else if class < 85 {
            (64, 1024)
        } else if class < 97 {
            (1024, 8192)
        } else {
            (8192, 16384)
        };
        tc.draw(gs::integers::<usize>().min_value(lo).max_value(hi))
    }

    fn draw_topic(tc: &TestCase) -> &'static str {
        TOPICS[tc.draw(gs::integers::<usize>().min_value(0).max_value(TOPICS.len() - 1))]
    }

    fn draw_filter(tc: &TestCase) -> &'static str {
        FILTERS[tc.draw(gs::integers::<usize>().min_value(0).max_value(FILTERS.len() - 1))]
    }

    // --------------------------------------------------------------------------------
    // Startup
    // --------------------------------------------------------------------------------

    fn wait_until_up(&self) {
        let mut last = String::new();
        loop {
            let mut waiting = Vec::new();
            if let Err(e) = net::mqtt_probe(&self.broker_host, self.broker_port, Duration::from_secs(2)) {
                waiting.push(format!("broker: {e}"));
            }
            for c in &self.clients {
                if let Err(e) = net::get_health(&self.agent, &c.url) {
                    waiting.push(format!("{}: {e}", c.name));
                }
            }
            if waiting.is_empty() {
                log("broker and every client program answer");
                return;
            }
            let msg = waiting.join("; ");
            if msg != last {
                log(&format!("waiting for: {msg}"));
                last = msg;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    /// Every client is connected to the broker (seen by the driver through its events), and
    /// holds a warm-up subscription, so each timeline starts with traffic flowing.
    fn wait_until_serving(&self) {
        let mut last = String::new();
        loop {
            // Clients that connected before the driver was listening sent their
            // `connected` event into the void; their health answer carries the same facts.
            for (i, c) in self.clients.iter().enumerate() {
                let Ok(h) = net::get_health(&self.agent, &c.url) else { continue };
                if h["connected"].as_bool() != Some(true) {
                    continue;
                }
                let (boot, conn, sp) = (h["boot"].as_u64().unwrap_or(0), h["conn"].as_u64().unwrap_or(0), h["sp"].as_bool().unwrap_or(false));
                let known = self.model.lock().unwrap().clients[i].conns.contains_key(&model::ConnKey { boot, conn });
                if !known && conn > 0 {
                    self.model.lock().unwrap().on_client_event(i, &json!({"k": "connected", "client": c.name, "boot": boot, "conn": conn,
                        "attempt": conn, "session_present": sp, "sp": sp, "seq": 0}));
                }
            }
            let waiting: Vec<String> = {
                let m = self.model.lock().unwrap();
                m.clients
                    .iter()
                    .filter(|c| !c.connected || c.boot.is_none())
                    .map(|c| c.name.clone())
                    .collect()
            };
            if waiting.is_empty() {
                break;
            }
            let msg = format!("clients not yet connected: {}", waiting.join(", "));
            if msg != last {
                log(&msg);
                last = msg;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        // Deterministic warm-up subscriptions: one exact topic and one wildcard per client.
        let mut ops = Vec::new();
        for ci in 0..self.clients.len() {
            let exact = TOPICS[ci % TOPICS.len()];
            let wild = ["t/a/+", "t/+/x", "t/#", "will/#", "#"][ci % 5];
            ops.push(self.subscribe_one(ci, exact, (((ci % 2) + 1) as u8).min(self.max_sub_qos)));
            ops.push(self.subscribe_one(ci, wild, ((ci % 3) as u8).min(self.max_sub_qos)));
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let done = {
                let m = self.model.lock().unwrap();
                ops.iter().all(|op| m.outcomes.get(op).map_or(false, |o| o == "suback"))
            };
            if done {
                log("warm-up subscriptions confirmed");
                return;
            }
            if Instant::now() > deadline {
                log("warm-up subscriptions not all confirmed within 60 s; continuing");
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

#[hegel::concurrent_state_machine]
impl Driver {
    /// One action per step, drawn with explicit probabilities (hegel's rule weights are only
    /// a hint, and the disconnect rate must stay low).
    #[rule]
    fn step(&self, tc: TestCase) {
        let pick: u32 = tc.draw(gs::integers::<u32>().min_value(0).max_value(999));
        if pick < 600 {
            self.act_publish(&tc);
        } else if pick < 720 {
            self.act_publish_burst(&tc);
        } else if pick < 900 {
            self.act_subscribe(&tc);
        } else if pick < 997 {
            self.act_unsubscribe(&tc);
        } else {
            self.act_disconnect(&tc);
        }
    }

    #[invariant(always_run)]
    fn maintenance(&self, tc: TestCase) {
        let quiet = tc.draw(gs::weighted_booleans(self.quiet_p));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut m = self.model.lock().unwrap();
            m.sweep();
            if round % 2000 == 0 {
                let rules: Vec<u64> = self.rule_counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();
                log(&format!("round {round}, {:.0} s, rules(pub,burst,sub,unsub,disc)={rules:?}: {}", self.started.elapsed().as_secs_f64(), m.summary()));
            }
        }
        if quiet {
            self.quiet_period(&tc);
        }
    }
}

impl Driver {
    fn act_publish(&self, tc: &TestCase) {
        self.rule_counts[0].fetch_add(1, Ordering::Relaxed);
        let ci = self.draw_client(tc);
        let topic = Self::draw_topic(tc);
        let qos = self.draw_qos(tc);
        let retain = tc.draw(gs::weighted_booleans(0.15));
        let empty = tc.draw(gs::weighted_booleans(0.05));
        let size = if empty { 0 } else { Self::draw_size(tc) };
        self.publish_one(ci, topic, qos, retain, size, empty);
    }

    fn act_publish_burst(&self, tc: &TestCase) {
        self.rule_counts[1].fetch_add(1, Ordering::Relaxed);
        let ci = self.draw_client(tc);
        let topic = Self::draw_topic(tc);
        let qos = self.draw_qos(tc);
        let n: usize = tc.draw(gs::integers::<usize>().min_value(3).max_value(12));
        let sizes: Vec<usize> = (0..n).map(|_| Self::draw_size(tc)).collect();
        for size in sizes {
            self.publish_one(ci, topic, qos, false, size, false);
        }
    }

    fn act_subscribe(&self, tc: &TestCase) {
        self.rule_counts[2].fetch_add(1, Ordering::Relaxed);
        let ci = self.draw_client(tc);
        let filter = Self::draw_filter(tc);
        let qos: u8 = tc.draw(gs::integers::<u8>().min_value(0).max_value(self.max_sub_qos));
        let already = self.model.lock().unwrap().live_filters(ci).iter().any(|f| f == filter);
        if already && self.model.lock().unwrap().active {
            assert_sometimes!(true, "a client re-subscribes to a filter it already holds (MQTT-3.8.4-3)", &json!({"filter": filter}));
        }
        self.subscribe_one(ci, filter, qos);
    }

    fn act_unsubscribe(&self, tc: &TestCase) {
        self.rule_counts[3].fetch_add(1, Ordering::Relaxed);
        let ci = self.draw_client(tc);
        let pick: usize = tc.draw(gs::integers::<usize>().min_value(0).max_value(9));
        let unknown = tc.draw(gs::weighted_booleans(0.1));
        let live = self.model.lock().unwrap().live_filters(ci);
        let filter = if unknown || live.is_empty() {
            FILTERS[pick].to_string()
        } else {
            live[pick % live.len()].clone()
        };
        self.unsubscribe_one(ci, &filter);
    }

    fn act_disconnect(&self, tc: &TestCase) {
        self.rule_counts[4].fetch_add(1, Ordering::Relaxed);
        let ci = self.draw_client(tc);
        self.disconnect_one(ci);
    }
}

fn parse_clients() -> Vec<ClientInfo> {
    // CLIENTS=client1=persistent,client2=clean,...  (URL http://<name>:8080)
    let spec = std::env::var("CLIENTS").unwrap_or_else(|_| "client1=persistent,client2=clean".to_string());
    spec.split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (name, mode) = s.trim().split_once('=').unwrap_or((s.trim(), "clean"));
            ClientInfo {
                name: name.to_string(),
                url: format!("http://{name}:8080"),
                persistent: mode == "persistent",
            }
        })
        .collect()
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_init();
    let clients = parse_clients();
    let strict_qos = std::env::var("MQTT_STRICT_QOS").map(|v| v != "0").unwrap_or(true);
    let quiet_p: f64 = std::env::var("QUIET_P").ok().and_then(|v| v.parse().ok()).unwrap_or(1.0 / 20000.0);
    let broker_host = std::env::var("BROKER_HOST").unwrap_or_else(|_| "broker".into());
    let broker_port: u16 = std::env::var("BROKER_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(1883);
    let max_sub_qos: u8 = std::env::var("MAX_SUB_QOS").ok().and_then(|v| v.parse().ok()).unwrap_or(2).min(2);
    let max_pub_qos: u8 = std::env::var("MAX_PUB_QOS").ok().and_then(|v| v.parse().ok()).unwrap_or(2).min(2);

    let model = Arc::new(Mutex::new(Model::new(
        clients.iter().map(|c| (c.name.clone(), c.url.clone(), c.persistent)).collect(),
        strict_qos,
    )));
    // Receivers first, so every line and event sent after this point is seen.
    net::start_log_receiver(7777, model.clone());
    let names = clients.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect();
    net::start_event_receiver(7778, model.clone(), names);

    let driver = Driver {
        model: model.clone(),
        agent: net::agent(),
        next_op: AtomicU64::new(1),
        clients,
        quiet_p,
        rounds: AtomicU64::new(0),
        quiet_count: AtomicU64::new(0),
        broker_host,
        broker_port,
        started: Instant::now(),
        max_sub_qos,
        max_pub_qos,
        rule_counts: Default::default(),
    };
    log(&format!(
        "clients: {}; strict_qos={strict_qos}; quiet_p={quiet_p}; max_sub_qos={max_sub_qos}; max_pub_qos={max_pub_qos}",
        driver.clients.iter().map(|c| format!("{}({})", c.name, if c.persistent { "persistent" } else { "clean" })).collect::<Vec<_>>().join(", ")
    ));
    driver.wait_until_up();
    driver.wait_until_serving();
    antithesis_sdk::lifecycle::setup_complete(&json!({
        "clients": driver.clients.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
        "strict_qos": strict_qos,
    }));
    log("setup complete");

    while !std::path::Path::new(WORKLOAD_ACTIVE).exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    model.lock().unwrap().active = true;
    assert_reachable!("the workload starts", &json!({}));
    log("workload active");

    hegel::stateful::machine(driver)
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}
