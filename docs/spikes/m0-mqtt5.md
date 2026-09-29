# M0: MQTT 5 properties

Spike 0 from `device-agent-spec.md` section 5.1. It answers whether the broker
supports the MQTT 5 features M2 is designed around, by measuring them rather
than by reading release notes.

Two programs, neither part of the agent: nothing under `cmd/skuhus-device-serial-scanner`
imports them and `make build` does not build them. `spike/brokerinfo` says what
a broker is and which protocol levels it answers; `spike/mqtt5` measures the
properties on a broker that answers MQTT 5.

## Status: the fleet broker does not speak MQTT 5, and two properties fail even on 4.1.8

Measured against `rabbitmq:4.1-management` (4.1.8) with `rabbitmq_mqtt` enabled,
run locally in Docker as a single node and again as a two-node cluster, and
repeated against the 4.3.5 development broker in `dev/rabbitmq/`, which gives
the same result on every check.

Message expiry, retained publish and clearing, request-response properties and
the last will itself all behave as section 5 assumes. Two do not, and both
concern the retained status topic in section 5.5:

- A last will is delivered but never retained.
- A retained message published through one cluster node is invisible on another.

Section 5.1 says that if any of these do not hold, the fallback is a dedicated
broker bridged to RabbitMQ, and the specification needs revising before M2.
Whether that is warranted depends on open question 1 (fleet topology) and on how
the ingest side decides a station is down, so it is not decided here. The
cheaper alternatives are in "What this costs M2" below.

The fleet broker at 10.9.21.23 has now been reached, and it is not the broker
the specification assumes. It is **RabbitMQ 3.10.25**, and it does not support
MQTT 5 at all: see "The fleet broker speaks MQTT 3.1.1 only" below. Every
property measured in this document was measured against a local 4.1.8, because
the deployed broker cannot answer the questions.

## The fleet broker speaks MQTT 3.1.1 only

Measured with `spike/brokerinfo`, which sends a CONNECT by hand at each protocol
level. A broker that does not support a level closes the connection without a
CONNACK, and a client library reports that as `EOF`, which reads like a network
fault rather than an answer.

```
mqtt 3.1.1 CONNACK reason 0x00 (accepted; this broker takes unauthenticated clients)
mqtt 5.0   no CONNACK, connection closed (EOF): this protocol level is not supported
amqp       product RabbitMQ version 3.10.25 platform Erlang/OTP 25.3.2.9
           cluster rmq-diego@juan.development-operations.com
```

The same probe against the local 4.1.8 answers both levels, which is what rules
out a malformed probe packet:

```
mqtt 3.1.1 CONNACK reason 0x04 (bad user name or password)
mqtt 5.0   CONNACK reason 0x86 (bad user name or password)
amqp       product RabbitMQ version 4.1.8 platform Erlang/OTP 27.3.4.17
```

RabbitMQ added MQTT 5 in 3.13, so 3.10.25 having none of it is expected rather
than a misconfiguration. Section 5.2 requires MQTT 5 and names `paho.golang`
specifically because `paho.mqtt.golang` is 3.1.1 and lacks the properties. On
this broker as deployed, the agent cannot connect at all.

What MQTT 3.1.1 would cost, if the broker stays as it is:

- **Message expiry does not exist.** Section 6's perishability is enforced by
  the broker in the current design. Without it, a stale scan is discarded by the
  consumer, on trust, or by an AMQP-side TTL applied to the queue rather than to
  the message.
- **Response topic and correlation data do not exist.** Section 5.7's command
  correlation would move into the JSON payload, and the reply topic would be
  fixed by convention rather than named by the requester.
- **Session expiry and clean start semantics are the 3.1.1 ones.** Section 5.2's
  "clean start true, session expiry 0" becomes "clean session true", which is
  close enough in effect.
- Retained messages and LWT do exist in 3.1.1, with the same two failures
  measured below.

The cluster name is set (`rmq-diego@juan.development-operations.com`), but that
says nothing about node count: a single node reports one too. Whether the
retained-store finding below applies here depends on that count, which the
management API would answer. Port 15672 is closed from this host; 1883 and 5672
are open.

**The broker accepts unauthenticated MQTT connections.** The 3.1.1 probe sent no
credentials and was accepted with reason 0x00, from off-host. Section 8 asks for
per-station credentials; today anything that can reach the port can publish to
any topic. The local 4.1.8, configured with a non-default user, refuses the same
probe.

## Results

```
PASS connack          retain_available=true max_qos=1 session_expiry=0
                      wildcard_sub=true shared_sub=false sub_id=true
PASS retained         delivered to a late subscriber with retain=true, and
                      cleared by a zero length publish
PASS request-response response topic and correlation data relayed unchanged in
                      both directions
FAIL retained-cluster retained message published through node 1 was not
                      delivered to a subscriber on node 2, while a live message
                      on the same topic crossed to the same subscriber
PASS lwt              will delivered to a live subscriber after socket close
FAIL lwt-retained     a subscriber connecting after the will was published
                      received nothing
PASS expiry           the 2s message expired during a 6s absence, the 300s one
                      was delivered: fresh (remaining expiry 293s)
```

### Message expiry works, and the remaining interval is decremented

The check queues two QoS 1 messages for a subscriber that has disconnected while
holding a session (session expiry 300s), one with a 2s message expiry and one
with 300s, waits 6s, and reconnects. Only the long-lived message is delivered,
and it arrives with 293s remaining rather than the original 300s.

This is the mechanism section 6 relies on: a scan that outlived its session is
discarded by the broker, not by a consumer.

### Maximum QoS is 1

The broker reports `max_qos=1` in CONNACK, so QoS 2 is unavailable. The agent
does not want it: scans, status and commands are QoS 1, heartbeats QoS 0, and
QoS 2 would pay two extra round trips for a guarantee that `event_id` dedup
already provides.

### Response topic and correlation data survive a round trip

Both directions were checked: the properties reach the subscriber as sent, and a
reply published to the topic the request named, carrying the same correlation
data, reaches the original requester. This is what section 5.7 needs to match a
command result to its command.

### Retained publish works at QoS 1, and a zero length payload clears it

A subscriber connecting after the publish receives the message with the retain
flag set, and a subsequent zero length retained publish removes it rather than
storing an empty message. That is the shape of section 5.5's status topic and
its clean-shutdown clear, on one node.

### A will is delivered but not retained

The will is published with `retain: true`, arrives at a subscriber that was
already connected, and is then absent for a subscriber that connects immediately
afterwards. Measured at both will QoS 0 and QoS 1 (`--will-qos`), while an
ordinary retained publish on the same connection and the same broker is stored
correctly, so this is specific to wills rather than to retention.

Repeated on RabbitMQ 4.3.5, where it behaves the same, so this is not a defect
of one release that a broker upgrade would carry away.

The consequence for section 5.5 is specific. Publishing `online` retained at
startup and relying on the will to overwrite it leaves the last retained status
of a dead station reading `online` indefinitely. Only consumers connected at the
moment of death see the offline transition; whoever subscribes later is told the
station is up.

### Retained messages do not cross cluster nodes

Published retained through node 1 of a two-node cluster, subscribed on node 2:
nothing arrives. A live, non-retained message published on the same topic, to
the same subscription, immediately afterwards does arrive, so this is the
retained store rather than cross-node routing.

`rabbitmqctl environment` names the store: `{retained_message_store,
rabbit_mqtt_retained_msg_store_dets}`, with a 2s sync interval. A DETS table is
a file on one node, not replicated state, which is exactly the behaviour section
5.1 warns about. Which node a consumer's connection lands on then decides
whether it sees a station's retained status at all.

This matters only if the fleet broker is clustered, which is open question 1.

Other settings recorded from the same output, for the M2 design:
`max_session_expiry_interval_seconds` 86400, `topic_alias_maximum` 16,
`max_packet_size_authenticated` 16777216, `prefetch` 10, exchange `amq.topic`.
Shared subscriptions are reported unavailable (`shared_sub=false`); the agent
does not use them.

## What this costs M2

Both failures land on the same feature: retained status as a reliable way to ask
"is station pack-03 up". Retained status still works for a consumer on the same
node as the publisher while the agent is alive, so what is lost is the offline
case and the clustered case.

These sit behind the larger question of which broker M2 targets at all. If the
deployed 3.10.25 is upgraded to 3.13 or later, everything measured on 4.1.8
applies and the two failures below are what remains. If it is not, M2 is an
MQTT 3.1.1 design and section 5 needs rewriting first.

Options for the retained-status problem, in increasing order of cost. None is
chosen here, because the choice belongs with the ingest side and with open
question 1:

- Treat the heartbeat (section 5.6) as the liveness signal and retained status
  as informational, so "no heartbeat from pack-03 in 5 minutes" is what marks a
  station down. The heartbeat is already specified, and this needs no change to
  the agent and no change to the broker.
- Have the ingest side republish a retained `offline` when it observes a will,
  putting the correction where the broker will not do it.
- Take section 5.1's fallback: a dedicated MQTT broker bridged to RabbitMQ. This
  is the only option that makes retained status trustworthy on its own, and it
  is a new piece of infrastructure per site.

## Reproducing

Against the development broker in `dev/rabbitmq/`, which is 4.3.5 and has
per-station credentials, so the run also exercises the topic permissions:

```
make broker-up
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03"
```

Dropping the `--prefix` is how to see those permissions work: the same user
outside its own station tree fails to subscribe at all.

Two-node cluster, which is what the `retained-cluster` check needs:

```
make spike-cluster-up
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03 --peer skuhus-dev-rabbitmq-2:1883"
make broker-down
```

Against another broker, pass the address and credentials:

```
make spike-mqtt5 BROKER=10.9.21.23:1883 MQTT_USER=guest MQTT_PASS=guest
```

Identify an unfamiliar broker first, because `spike-mqtt5` cannot tell a broker
that refuses MQTT 5 from a network fault:

```
make spike-brokerinfo BROKER=10.9.21.23:1883 FLAGS="--amqp 10.9.21.23:5672"
```

The harness creates its topics under `skuhus/spike/<random>`, so one run cannot
read another's leftovers, and it clears the retained messages it sets. The will
topic needs no clearing on this broker, because nothing was retained there.
