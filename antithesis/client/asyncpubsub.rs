use tokio::sync::Notify;
use tokio::{task, time};

use antithesis_sdk::{antithesis_init, lifecycle};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::json;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

const TOPIC_FILTER: &str = "chat/#";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    antithesis_init();

    let host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".into());
    let port: u16 = env::var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);
    let client_id = env::var("CLIENT_ID").unwrap_or_else(|_| "client".into());

    println!("[{client_id}] connecting to {host}:{port}");

    let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Publishing starts only after the broker acknowledges our subscription,
    // so we never miss messages published before we were subscribed.
    let subscribed = Arc::new(Notify::new());
    let mut setup_signaled = false;
    task::spawn(publish_loop(
        client.clone(),
        client_id.clone(),
        subscribed.clone(),
    ));

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                // Clean session: re-subscribe on every (re)connect.
                println!("[{client_id}] connected, subscribing to {TOPIC_FILTER}");
                client.try_subscribe(TOPIC_FILTER, QoS::AtLeastOnce)?;
            }
            Ok(Event::Incoming(Packet::SubAck(_))) => {
                println!("[{client_id}] subscribed");
                subscribed.notify_one();
                // First SubAck proves the broker is reachable and routing;
                // reconnects re-subscribe, so only signal once per process.
                if !setup_signaled {
                    lifecycle::setup_complete(&json!({ "client_id": client_id }));
                    setup_signaled = true;
                }
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                println!(
                    "[{client_id}] received on {}: {}",
                    p.topic,
                    String::from_utf8_lossy(&p.payload)
                );
            }
            Ok(_) => {}
            Err(e) => {
                // Broker not up yet or connection dropped: retry.
                println!("[{client_id}] connection error: {e:?}; retrying in 1s");
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn publish_loop(client: AsyncClient, client_id: String, subscribed: Arc<Notify>) {
    subscribed.notified().await;

    let topic = format!("chat/{client_id}");
    for seq in 0u64.. {
        let payload = format!("hello from {client_id} #{seq}");
        println!("[{client_id}] publishing on {topic}: {payload}");
        if let Err(e) = client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
        {
            println!("[{client_id}] publish failed: {e:?}");
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}
