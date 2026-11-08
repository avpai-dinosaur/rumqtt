//! QoS 2 exactly-once workload.
//!
//! ROLE=publisher: publishes (run_id, idx, crc, bytes) to TOPIC with
//!   strictly increasing idx.
//! ROLE=subscriber: checks every message for corruption, duplicates and gaps.
//!
//! Both sides use persistent sessions (clean_session = false), since QoS 2's
//! exactly-once guarantee only holds within a session. When a session is lost
//! (broker restart => session_present = false, or a publisher restart => new
//! run_id) loss/duplication is allowed by the spec, so the subscriber resets
//! its expectations instead of asserting.

use tokio::{task, time};

use antithesis_sdk::{antithesis_init, assert_always, assert_sometimes, lifecycle, random};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::json;
use std::env;
use std::time::Duration;

const TOPIC: &str = "exactly-once";
// run_id (8) | idx (8) | crc32 (4) | random bytes
const HEADER_LEN: usize = 20;
const MAX_BYTES: u64 = 256;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();

    let host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".into());
    let port: u16 = env::var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);
    let mut client_id = env::var("CLIENT_ID").unwrap_or_else(|_| "client".into());
    let role = env::var("ROLE").unwrap_or_else(|_| "subscriber".into());

    // A fresh run_id per publisher process lets the subscriber tell a publisher
    // restart (idx starts over, in-flight messages may be lost) from a duplicate.
    // It is also part of the client id: a restarted process has lost its QoS 2
    // packet-id state, so resuming the old session would make the broker
    // legitimately drop new messages that reuse in-flight packet ids.
    let run_id = random::get_random();
    if role == "publisher" {
        client_id = format!("{client_id}-{run_id:016x}");
    }

    println!("[{client_id}] {role} connecting to {host}:{port}");

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_session(false);

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

    match role.as_str() {
        "publisher" => run_publisher(client, eventloop, client_id, run_id).await,
        "subscriber" => run_subscriber(client, eventloop, client_id).await,
        other => panic!("unknown ROLE {other:?}, expected publisher or subscriber"),
    }
}

fn crc(run_id: u64, idx: u64, bytes: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&run_id.to_be_bytes());
    h.update(&idx.to_be_bytes());
    h.update(bytes);
    h.finalize()
}

fn encode(run_id: u64, idx: u64) -> Vec<u8> {
    let len = (random::get_random() % MAX_BYTES + 1) as usize;
    let bytes: Vec<u8> = (0..len).map(|_| random::get_random() as u8).collect();

    let mut msg = Vec::with_capacity(HEADER_LEN + len);
    msg.extend_from_slice(&run_id.to_be_bytes());
    msg.extend_from_slice(&idx.to_be_bytes());
    msg.extend_from_slice(&crc(run_id, idx, &bytes).to_be_bytes());
    msg.extend_from_slice(&bytes);
    msg
}

async fn run_publisher(
    client: AsyncClient,
    mut eventloop: EventLoop,
    client_id: String,
    run_id: u64,
) {
    println!("[{client_id}] run_id={run_id}");

    let id = client_id.clone();
    task::spawn(async move {
        for idx in 1u64.. {
            let msg = encode(run_id, idx);
            // An Err here means the request never entered rumqttc's queue, so
            // retrying the same idx can't duplicate it; skipping it would
            // look like loss to the subscriber.
            while let Err(e) = client
                .publish(TOPIC, QoS::ExactlyOnce, false, msg.clone())
                .await
            {
                println!("[{id}] publish idx={idx} failed: {e:?}; retrying");
                time::sleep(Duration::from_millis(200)).await;
            }
            time::sleep(Duration::from_millis(200)).await;
        }
    });

    let mut connected_before = false;
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                println!("[{client_id}] connected, session_present={}", ack.session_present);
                if connected_before {
                    // rumqttc discards unacked and queued publishes when the
                    // session is gone, so the subscriber may see a gap next.
                    // Expected after a broker restart; otherwise the broker
                    // dropped a live session.
                    if !ack.session_present {
                        println!("[{client_id}] session lost on reconnect; in-flight messages discarded");
                    }
                    assert_sometimes!(
                        !ack.session_present,
                        "Publisher lost its session on reconnect",
                        &json!({ "run_id": run_id })
                    );
                }
                connected_before = true;
            }
            Ok(_) => {}
            Err(e) => {
                println!("[{client_id}] connection error: {e:?}; retrying in 1s");
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_subscriber(client: AsyncClient, mut eventloop: EventLoop, client_id: String) {
    // (run_id, idx) of the last message seen in the current session.
    let mut last: Option<(u64, u64)> = None;
    let mut need_subscribe = false;
    let mut resumed_session = false;
    let mut setup_signaled = false;

    loop {
        // Retried every pass so a full request queue can't drop the subscribe.
        if need_subscribe && client.try_subscribe(TOPIC, QoS::ExactlyOnce).is_ok() {
            need_subscribe = false;
        }

        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                println!("[{client_id}] connected, session_present={}", ack.session_present);
                resumed_session = ack.session_present;
                if !ack.session_present {
                    // Broker has no session for us: subscription and any
                    // queued messages are gone, so gaps are expected.
                    println!("[{client_id}] no session on broker; subscribing and resetting expectations");
                    need_subscribe = true;
                    last = None;
                }
            }
            Ok(Event::Incoming(Packet::SubAck(_))) => {
                println!("[{client_id}] subscribed to {TOPIC}");
                if !setup_signaled {
                    lifecycle::setup_complete(&json!({ "client_id": client_id }));
                    setup_signaled = true;
                }
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                check_message(&p.payload, p.dup, p.pkid, resumed_session, &mut last, &client_id);
            }
            Ok(_) => {}
            Err(e) => {
                println!("[{client_id}] connection error: {e:?}; retrying in 1s");
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn check_message(
    payload: &[u8],
    dup: bool,
    pkid: u16,
    resumed_session: bool,
    last: &mut Option<(u64, u64)>,
    client_id: &str,
) {
    let well_formed = payload.len() > HEADER_LEN;
    assert_always!(
        well_formed,
        "Subscriber receives well-formed messages",
        &json!({ "len": payload.len() })
    );
    if !well_formed {
        return;
    }

    let run_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let idx = u64::from_be_bytes(payload[8..16].try_into().unwrap());
    let sent_crc = u32::from_be_bytes(payload[16..20].try_into().unwrap());
    let bytes = &payload[HEADER_LEN..];
    let actual_crc = crc(run_id, idx, bytes);

    println!(
        "[{client_id}] received run_id={run_id} idx={idx} len={} dup={dup} pkid={pkid}",
        bytes.len()
    );

    assert_always!(
        sent_crc == actual_crc,
        "Message payload is not corrupted",
        &json!({ "run_id": run_id, "idx": idx, "sent_crc": sent_crc, "actual_crc": actual_crc })
    );

    match *last {
        Some((last_run, last_idx)) if last_run == run_id => {
            let details = json!({
                "run_id": run_id, "idx": idx, "last_idx": last_idx,
                "dup": dup, "pkid": pkid, "resumed_session": resumed_session,
            });
            assert_always!(idx > last_idx, "QoS 2 message is not delivered twice", &details);
            assert_always!(
                idx <= last_idx + 1,
                "QoS 2 message is not lost within a session",
                &details
            );
            assert_sometimes!(
                resumed_session,
                "Subscriber receives a message after resuming its session",
                &details
            );
            assert_sometimes!(dup, "Subscriber receives a redelivered (DUP) message", &details);
            if idx > last_idx {
                *last = Some((run_id, idx));
            }
        }
        _ => {
            // First message of this process/session/publisher run: nothing to compare against.
            println!("[{client_id}] baseline run_id={run_id} idx={idx}");
            *last = Some((run_id, idx));
        }
    }
}
