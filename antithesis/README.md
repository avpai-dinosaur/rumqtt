# Antithesis harness for rumqttd

This directory holds everything needed to test `rumqttd` (through `rumqttc`) in
Antithesis: a Docker Compose deployment, an instrumented broker and client build, and a
driver that generates MQTT traffic and checks the broker's behaviour against the MQTT
3.1.1 specification.

## Layout

| Path | What it is |
| --- | --- |
| `docker-compose.yaml` | The deployment: one `broker`, five `client` containers, one fault-excluded `driver`. |
| `docker-compose.local.yaml` | Local overrides: SDK assertion output to a file, frequent quiet periods. |
| `broker/` | Wrapper crate that builds `rumqttd` with Antithesis coverage instrumentation and single-line JSON logs. |
| `client/` | One long-lived `rumqttc` client per container, driven over HTTP, reporting every outcome over UDP. Instrumented. |
| `driver/` | The hegel state machine: rules, model, property checks, quiet period. Also the log forwarder. |
| `patches/sut-assertions.patch` | Exploration assertions inside `rumqttd` and `rumqttc`, applied only by the Antithesis build. |

## How it works

- **Clients** (`client1`, `client2`, `client5` persistent sessions; `client3`, `client4`
  clean sessions) each hold one `rumqttc` connection with a fixed client id, a keep-alive
  of 5 s, and a will message unique to each CONNECT attempt. They accept operations on
  `POST /op` and report connection changes, wire sends, acknowledgements and deliveries to
  the driver on UDP 7778.
- **The driver** picks operations at random (publish, bursts, subscribe, unsubscribe,
  graceful disconnect) across five topics and ten filters (exact, `+`, `#`, `will/#`),
  records what it sent before each request goes out, and checks every event against the
  model. The broker's and clients' logs reach it on UDP 7777.
- **Quiet periods** pause faults, wait for every client to be served again, then check the
  guarantees that only hold once faults stop (backlog delivery, message flow, retained
  delivery, UNSUBACK).

## Properties

Guarantees (`always` / `unreachable`), with the spec clause they come from:

- a delivered payload is intact; a delivered message was published by the workload
- a message is delivered only to a matching subscription that was live when it was
  published (3.3.2-3, 3.10.4-2); a will message only to a matching live subscription
- a QoS 2 message is delivered exactly once per matching subscription (4.3.3); a QoS 0
  message at most once (4.3.1)
- a subscriber receives one publisher's messages to a topic in the order they were sent
  (4.6.0-6)
- a forwarded message's QoS never exceeds the subscription's granted QoS, and (with
  `MQTT_STRICT_QOS=1`) never the QoS it was published with (3.8.4-6)
- a retained message delivered to a new subscription is the topic's latest retained
  message (3.3.1-6); a message delivered with the retain flag was published retained
- a SUBACK carries one return code per filter and grants at most the requested QoS (3.8.4-5)
- a clean-session connection never reports session present (3.2.2-1); a persistent session
  survives as long as the broker stays up (3.1.2-4)
- the broker never publishes the will of a connection that is still alive (3.1.2-8)
- once faults stop: every client is served again; an acknowledged QoS 1/2 message reaches
  every matching live subscription (4.5.0-1); a message published after recovery reaches
  every live matching subscription; a new subscription receives the retained message
  (3.3.1-6); a non-retained publish leaves the retained message in place (3.3.1-12); every
  UNSUBSCRIBE is answered (3.10.4-4)
- from the logs: the broker never panics, its router thread never exits, it never
  receives an unsolicited or out-of-order ack, never rejects a workload publish
- inside the SUT (patch): a PUBREL never arrives without a recorded QoS 2 publish, a
  PUBACK/PUBREC always names the head of the inflight queue, a PUBCOMP always has a
  pending PUBREL

Exploration assertions (`sometimes` / `reachable`) mark the states worth reaching:
reconnects, resumed and lost sessions, retransmissions, duplicates, wills, retained
deliveries, overlapping and wildcard subscriptions, broker restarts, takeovers, and the
retry/recovery branches inside rumqttd and rumqttc.

## Running locally

```bash
cd antithesis
docker compose build broker client1 driver
export COMPOSE_FILE=docker-compose.yaml:docker-compose.local.yaml
docker compose down -v && docker compose up -d
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
docker compose logs -f driver
# after a few minutes:
docker compose cp driver:/tmp/sdk.jsonl .
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
  | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
           else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
  | "\(.[0].display_type)\t\(.[0].message)"' sdk.jsonl
docker compose down -v
```

## Launching in Antithesis

```bash
cd antithesis
docker compose build broker client1 driver
snouty validate .
snouty launch --launcher basic_test --config . \
  --test-name rumqttd --duration 30m \
  --fault-tolerance resilient --fault-exclusions '^driver$'
```

Use `--fault-tolerance high_availability` to add container kills and partitions. The
broker keeps all state in memory, so a kill loses every session; the model accounts for
that through the `session_present` flag and the broker's boot id in its forwarded logs.

`MQTT_STRICT_QOS=0` on the driver relaxes the two checks that depend on the spec's
"forward at min(published QoS, granted QoS)" rule, which rumqttd does not implement (it
forwards at the subscription's QoS).
