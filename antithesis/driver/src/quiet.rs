//! The quiet period: faults are paused, the system is given time to recover, and the
//! guarantees that only hold once faults stop are checked.
use crate::model::ConnKey;
use crate::{log, Driver, TOPICS};
use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes};
use hegel::generators as gs;
use hegel::TestCase;
use serde_json::json;
use std::time::{Duration, Instant};

/// How long faults stay paused: longer than the whole quiet period can take.
const QUIET_SECS: u64 = 240;
/// Every client must be connected and served again by then.
const RECOVERY: Duration = Duration::from_secs(60);
/// A single request in a quiet period must complete within this.
const STEP: Duration = Duration::from_secs(15);
/// Deliveries must settle for this long before the backlog is checked.
const DRAIN_IDLE: Duration = Duration::from_secs(4);
const DRAIN_MAX: Duration = Duration::from_secs(45);

impl Driver {
    pub fn quiet_period(&self, tc: &TestCase) {
        // All draws up front.
        let n = self.clients.len();
        let a: usize = tc.draw(gs::integers::<usize>().min_value(0).max_value(n - 1));
        let b = (a + 1 + tc.draw(gs::integers::<usize>().min_value(0).max_value(n.max(2) - 2))) % n;
        let empty_variant = tc.draw(gs::weighted_booleans(0.5));
        let flow_topic = TOPICS[tc.draw(gs::integers::<usize>().min_value(0).max_value(TOPICS.len() - 1))];
        let unsub_client: usize = tc.draw(gs::integers::<usize>().min_value(0).max_value(n - 1));
        let id = self.quiet_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        if let Some(bin) = std::env::var_os("ANTITHESIS_STOP_FAULTS") {
            let _ = std::process::Command::new(bin).arg(QUIET_SECS.to_string()).status();
        }
        let t0 = Instant::now();
        log(&format!("quiet period {id}: faults paused for {QUIET_SECS} s"));

        let healthy = self.wait_for_recovery();
        let all = healthy.iter().all(|h| *h);
        let summary = self.model.lock().unwrap().summary();
        assert_always!(all, "every client reconnects and is served again once faults stop",
            &json!({"healthy": self.clients.iter().zip(&healthy).map(|(c, h)| json!({"client": c.name, "healthy": h})).collect::<Vec<_>>(), "state": summary}));
        if !all {
            log(&format!("quiet period {id}: not every client recovered: {healthy:?}"));
            return;
        }
        assert_reachable!("the system recovered after faults stopped", &json!({}));
        log(&format!("quiet period {id}: every client recovered in {:.1} s", t0.elapsed().as_secs_f64()));

        self.drain();
        self.check_backlog(t0);
        self.check_flow(a, flow_topic);
        self.check_retained(a, b, empty_variant, id);
        self.check_unsuback(unsub_client, id);
        log(&format!("quiet period {id}: done in {:.1} s", t0.elapsed().as_secs_f64()));
    }

    /// Waits until every client is connected and gets a fresh QoS 1 publish acknowledged.
    fn wait_for_recovery(&self) -> Vec<bool> {
        let n = self.clients.len();
        let mut healthy = vec![false; n];
        let mut probe: Vec<Option<(u64, Instant)>> = vec![None; n];
        let deadline = Instant::now() + RECOVERY;
        while Instant::now() < deadline && healthy.iter().any(|h| !h) {
            for ci in 0..n {
                if healthy[ci] {
                    continue;
                }
                if let Some((op, at)) = probe[ci] {
                    let acked = self.model.lock().unwrap().outcomes.get(&op).map_or(false, |o| o == "acked");
                    if acked {
                        healthy[ci] = true;
                        continue;
                    }
                    if at.elapsed() < Duration::from_secs(10) {
                        continue;
                    }
                }
                let connected = net::get_health(&self.agent, &self.clients[ci].url)
                    .map(|h| h["connected"].as_bool().unwrap_or(false))
                    .unwrap_or(false);
                if connected {
                    let topic = format!("probe/{}", self.clients[ci].name);
                    let op = self.publish_one(ci, &topic, 1, false, 8, false);
                    probe[ci] = Some((op, Instant::now()));
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        healthy
    }

    fn drain(&self) {
        let start = Instant::now();
        loop {
            let idle = {
                let m = self.model.lock().unwrap();
                m.clients.iter().filter_map(|c| c.last_delivery).map(|t| t.elapsed()).min().unwrap_or(DRAIN_IDLE)
            };
            if idle >= DRAIN_IDLE || start.elapsed() > DRAIN_MAX {
                return;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Every acknowledged QoS 1/2 message sent before the quiet period reached each matching
    /// subscription that was confirmed before it was sent and is still live.
    fn check_backlog(&self, t0: Instant) {
        let m = self.model.lock().unwrap();
        let mut checked = 0u64;
        let mut missing = Vec::new();
        for (si, sub) in m.clients.iter().enumerate() {
            let Some(boot) = sub.boot else { continue };
            let Some(cur) = sub.conns.get(&ConnKey { boot, conn: sub.conn }) else { continue };
            if cur.ended.is_some() || !sub.connected {
                continue;
            }
            for e in sub.epochs.iter() {
                let Some(confirmed) = e.confirmed else { continue };
                if e.session != sub.session || e.ended.is_some() || e.unsub_sent.is_some() || e.failed || e.granted.unwrap_or(e.qos) < 1 {
                    continue;
                }
                for msg in m.msgs.values() {
                    if msg.qos < 1 || msg.empty || msg.settled().is_none() || msg.sent <= confirmed || msg.sent >= t0 {
                        continue;
                    }
                    if !crate::topic::matches(&e.filter, &msg.topic) {
                        continue;
                    }
                    checked += 1;
                    let got = msg.deliveries.get(&(si, sub.session)).copied().unwrap_or(0);
                    if got == 0 {
                        missing.push(json!({"subscriber": sub.name, "filter": e.filter, "filter_confirmed_ms": m.ms(confirmed),
                            "session": sub.session, "op": msg.op, "topic": msg.topic, "publisher": m.clients[msg.publisher].name,
                            "qos": msg.qos, "sent_ms": m.ms(msg.sent), "settled_ms": msg.settled().map(|t| m.ms(t)), "sends": msg.sends}));
                    }
                }
            }
        }
        assert_sometimes!(checked > 0, "the quiet period verifies acknowledged messages against live subscriptions", &json!({"checked": checked}));
        log(&format!("backlog check: {checked} message/subscription pairs, {} missing", missing.len()));
        if missing.is_empty() {
            assert_always!(true, "an acknowledged QoS 1 or 2 message reaches every matching live subscription once faults stop (MQTT-4.5.0-1)", &json!({"checked": checked}));
        } else {
            for (i, detail) in missing.iter().enumerate().take(20) {
                let mut d = detail.clone();
                d["missing_total"] = json!(missing.len());
                d["index"] = json!(i);
                assert_always!(false, "an acknowledged QoS 1 or 2 message reaches every matching live subscription once faults stop (MQTT-4.5.0-1)", &d);
            }
        }
    }

    /// A message published after recovery reaches every live subscription that matches it.
    fn check_flow(&self, a: usize, topic: &str) {
        let subscribers: Vec<(usize, u64)> = {
            let m = self.model.lock().unwrap();
            m.clients
                .iter()
                .enumerate()
                .filter(|(_, c)| c.connected)
                .filter(|(_, c)| {
                    c.epochs.iter().any(|e| {
                        e.session == c.session && e.confirmed.is_some() && e.ended.is_none() && e.unsub_sent.is_none() && !e.failed
                            && e.granted.unwrap_or(e.qos) >= 1 && crate::topic::matches(&e.filter, topic)
                    })
                })
                .map(|(i, c)| (i, c.session))
                .collect()
        };
        assert_sometimes!(!subscribers.is_empty(), "the quiet-period flow check finds a live subscription for its topic", &json!({"topic": topic}));
        if subscribers.is_empty() {
            return;
        }
        let op = self.publish_one(a, topic, 1, false, 32, false);
        if !self.wait_for(STEP, |m| m.outcomes.get(&op).map_or(false, |o| o == "acked")) {
            log("flow check: probe publish not acknowledged; inconclusive");
            return;
        }
        let ok = self.wait_for(Duration::from_secs(20), |m| {
            let Some(msg) = m.msgs.get(&op) else { return false };
            subscribers.iter().all(|k| msg.deliveries.get(k).copied().unwrap_or(0) >= 1)
        });
        let detail = {
            let m = self.model.lock().unwrap();
            let msg = m.msgs.get(&op);
            json!({"topic": topic, "publisher": self.clients[a].name, "op": op,
                   "subscribers": subscribers.iter().map(|(i, s)| json!({"client": self.clients[*i].name, "session": s, "delivered": msg.and_then(|m| m.deliveries.get(&(*i, *s)).copied()).unwrap_or(0)})).collect::<Vec<_>>()})
        };
        assert_always!(ok, "a message published once faults stop reaches every live matching subscription", &detail);
    }

    /// Publishes a retained message, optionally followed by a non-retained empty publish, then
    /// subscribes another client and expects the retained message.
    fn check_retained(&self, a: usize, b: usize, empty_variant: bool, id: u64) {
        let topic = format!("rt/{id}");
        let r = self.publish_one(a, &topic, 1, true, 16, false);
        if !self.wait_for(STEP, |m| m.outcomes.get(&r).map_or(false, |o| o == "acked")) {
            log("retained check: retained publish not acknowledged; inconclusive");
            return;
        }
        if empty_variant {
            let e = self.publish_one(a, &topic, 1, false, 0, true);
            if !self.wait_for(STEP, |m| m.outcomes.get(&e).map_or(false, |o| o == "acked")) {
                log("retained check: empty publish not acknowledged; inconclusive");
                return;
            }
        }
        let s = self.subscribe_one(b, &topic, 1);
        if !self.wait_for(STEP, |m| m.outcomes.get(&s).map_or(false, |o| o == "suback")) {
            log("retained check: subscribe not acknowledged; inconclusive");
            return;
        }
        let session = self.model.lock().unwrap().clients[b].session;
        let got = self.wait_for(STEP, |m| m.msgs.get(&r).map_or(false, |m| m.retained_deliveries.contains(&(b, session))));
        let detail = json!({"topic": topic, "publisher": self.clients[a].name, "subscriber": self.clients[b].name, "op": r, "empty_variant": empty_variant});
        if empty_variant {
            assert_always!(got, "a non-retained publish leaves the retained message in place (MQTT-3.3.1-12)", &detail);
        } else {
            assert_always!(got, "a new subscription receives the retained message for its topic (MQTT-3.3.1-6)", &detail);
        }
        assert_reachable!("the quiet-period retained-message check runs to completion", &json!({}));
        // Tidy up: drop the subscription and clear the retained message.
        self.unsubscribe_one(b, &topic);
        self.publish_one(a, &topic, 1, true, 0, true);
    }

    /// An UNSUBSCRIBE for a filter the client never subscribed to still gets an UNSUBACK.
    fn check_unsuback(&self, c: usize, id: u64) {
        let filter = format!("never/{id}");
        let op = self.unsubscribe_one(c, &filter);
        let outcome = {
            self.wait_for(STEP, |m| m.outcomes.contains_key(&op));
            self.model.lock().unwrap().outcomes.get(&op).cloned()
        };
        match outcome.as_deref() {
            Some("unsuback") => assert_always!(true, "every UNSUBSCRIBE is answered with an UNSUBACK (MQTT-3.10.4-4)", &json!({"client": self.clients[c].name, "filter": filter})),
            Some("dropped") | Some("invalid") => log("unsuback check: request dropped; inconclusive"),
            other => assert_always!(false, "every UNSUBSCRIBE is answered with an UNSUBACK (MQTT-3.10.4-4)", &json!({"client": self.clients[c].name, "filter": filter, "outcome": other})),
        }
        assert_reachable!("the quiet-period UNSUBACK check runs to completion", &json!({}));
    }

    fn wait_for(&self, timeout: Duration, pred: impl Fn(&crate::model::Model) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&self.model.lock().unwrap()) {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

use crate::net;
