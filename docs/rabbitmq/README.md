# RabbitMQ for the device agent: an example deployment

An example of a RabbitMQ broker set up in advance for the agents and the
services around them. Checked on 2026-10-05 against RabbitMQ 4.3.5 and agent
2.0.0 (#34), and its broadcast group permissions on 2026-10-06 with `make
test-broker`. dev/rabbitmq/ is the development broker. This example has the
same users and permissions, in a vhost of its own, `skuhus`, where the
development broker uses `/`, and adds queues for an ingest service.

| File | Goes to | Holds |
|---|---|---|
| `definitions.example.json` | `/etc/rabbitmq/definitions.json` | the vhost, the users and their permissions, the queues and their bindings |
| `rabbitmq.example.conf` | `/etc/rabbitmq/rabbitmq.conf` | loading those definitions at boot, and the MQTT settings |
| `enabled_plugins` | `/etc/rabbitmq/enabled_plugins` | the MQTT and management plugins |

The passwords in the example are placeholders: `change-me-admin`,
`change-me-station` and `change-me-ingest`. Replace each hash with the output of
`rabbitmqctl hash_password <password>` before the file reaches a broker.

## What is set up

**The vhost** is `skuhus`. `mqtt.vhost = skuhus` makes it the vhost of every
MQTT username without a prefix, so an agent's username stays plain. A username
can also name its vhost itself, as `skuhus:station-pack-03`.

**The exchange** is `amq.topic`, which every vhost has. The MQTT plugin
publishes every MQTT message there (`mqtt.exchange`), with the topic as the
routing key, its `/` turned into `.`: `skuhus/acme/vasby/pack-03/scanner-main/rx`
is routed as `skuhus.acme.vasby.pack-03.scanner-main.rx`. Nothing else needs
declaring for MQTT. MQTT topics are routing keys, not objects, and the plugin
makes each subscriber's queue (`mqtt-subscription-<client id>qos1`) itself.

**The users:**
- `admin`, the administrator.
- `station-pack-03`, one per station, for its agent. Its topic permissions
  let it publish under its own station's topics only. It reads those, and its
  project's and its site's broadcast group tx topics (DESIGN-V2.md,
  "Broadcast groups"), which it must be able to read before its devices join
  a group there.
- `ingest`, for the services that read what the agents publish and send tx.
  It reads every topic and writes only tx topics, a device's or a broadcast
  group's. It has the `management` tag, which the HTTP API needs.

**The queues**, durable quorum queues for the ingest service, each bound to
`amq.topic`:

| Queue | Binding | Receives |
|---|---|---|
| `skuhus-ingest.rx` | `skuhus.*.*.*.*.rx` | every device's readings |
| `skuhus-ingest.device-status` | `skuhus.*.*.*.*.status` | every device's events and tx results |
| `skuhus-ingest.agent-status` | `skuhus.*.*.*.agent.*.status` | every agent's keepalives and offline messages |

A message waits in these queues until it is consumed or its message expiry
passes, and then the broker discards it. The agent sets the expiry
(DESIGN-V2.md, "Publishing"): the device's `message_expiry`, 30 s by default,
on readings, events and tx results, and `gone_after_s` on a keepalive. An
offline message has none, and neither has anything sent with curl over MQTT,
so those wait until they are consumed. A reading is perishable (DESIGN-V2.md,
"Scans are perishable: this agent does not do offline sync"), so an ingest
service that was stopped finds only the recent ones. On the check broker, with
a 5 s keepalive interval, `skuhus-ingest.agent-status` held 3 keepalives, each
with `expiration` 15000, after several minutes of them.

There is no queue for tx: each agent's own subscription receives them.

## Adding a station

For a station `pack-04` at the same site, add a user, its permissions and its
topic permissions, as `station-pack-03` has them, with the station in the
topic pattern:

```json
{ "user": "station-pack-04", "vhost": "skuhus", "exchange": "amq.topic",
  "write": "^skuhus\\.acme\\.vasby\\.pack-04\\..*$",
  "read": "^skuhus\\.acme\\.(vasby\\.pack-04\\..*|group\\.[^.]+\\.tx|vasby\\.group\\.[^.]+\\.tx)$" }
```

A station that tries another station's topics is refused, and the broker closes
its connection. Checked with `station-pack-03` publishing under `pack-04`: the
broker logged `MQTT topic access refused` and closed the connection "due to an
authorization failure".

## TLS

The agent refuses a plaintext broker URL unless `broker.insecure` is set. For
TLS, add to `rabbitmq.conf`:

```
mqtt.listeners.ssl.default = 8883
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile  = /etc/rabbitmq/tls/server.key
ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
```

and replace `mqtt.listeners.tcp.default = 1883` with `mqtt.listeners.tcp = none`.
With that line left in, the plaintext listener stays open beside the TLS one.
The agents connect to `tls://<broker>:8883`. `broker.ca_file` names the CA
bundle that the broker's certificate is checked against; left empty, the
system's CA certificates are used.

Checked with a CA and a certificate made for the check: the broker started only
the TLS listener, port 1883 refused connections, and the agent connected with
`ca_file`. The broker listed the connection as TLS 1.3, MQTT 5, in vhost
`skuhus`. The HTTP API stays plaintext on 15672 in this example.

## Sending a tx with curl

curl speaks MQTT. A tx is the JSON protocol/messages.schema.json defines as
`tx`, published on the device's tx topic, or on a broadcast group's, such as
`skuhus/acme/vasby/group/scales/tx` for the site's group `scales`, where each
device in the group answers on its own status topic:

```
curl -u ingest:<password> \
  -d '{"schema":2,"id":"5b1f6a0c-2d3e-4f40-9a1b-7c8d9e0f1a2b","sender":"curl","raw_b64":"EAQB"}' \
  mqtt://<broker>:1883/skuhus/acme/vasby/pack-03/printer-1/tx
```

Or through the HTTP API, with a user that has the `management` tag:

```
curl -u ingest:<password> -H 'content-type: application/json' \
  -X POST http://<broker>:15672/api/exchanges/skuhus/amq.topic/publish \
  -d '{"routing_key":"skuhus.acme.vasby.pack-03.printer-1.tx","payload_encoding":"string",
       "payload":"{\"schema\":2,\"id\":\"8c2d4e6f-1a3b-4c5d-8e9f-0a1b2c3d4e5f\",\"sender\":\"curl\",\"raw_b64\":\"EAQB\"}",
       "properties":{}}'
```

The API answers `{"routed":true}` when a queue took the message.

What curl cannot do:
- **It does not learn of a refusal.** curl publishes MQTT 3.1.1 at QoS 0, which
  has no acknowledgement: a publish the broker refused also exits 0. The tx's
  results are what say it arrived.
- **A tx sent with curl has no expiry.** MQTT 3.1.1 has no message expiry, and
  an `expiration` set through the HTTP API did not reach the agent as one: the
  agent logged `message_expiry` `none`. The agent never fails a tx without an
  expiry as `expired` (internal/core/tx.go, txJob.expired), so it waits its
  turn however long that takes. Senders are to set an expiry on every tx
  (DESIGN-V2.md, "tx"); `make send-tx` does, over MQTT 5, 30 s unless
  `--expiry` says otherwise.
- **It does not speak MQTT over TLS.** curl 8.14.1 answers `mqtts://` with
  `Protocol "mqtts" not supported`, so on a broker with only the TLS listener,
  curl sends a tx through the HTTP API.

## Reading the result

The results go to the device's status topic (DESIGN-V2.md, "tx results"). A tx
the agent can read gets `accepted`, then `written` or `failed`; one it cannot
read gets `failed` alone. The ingest queue holds them:

```
curl -u ingest:<password> -H 'content-type: application/json' \
  -X POST http://<broker>:15672/api/queues/skuhus/skuhus-ingest.device-status/get \
  -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}'
```

`ack_requeue_true` leaves the messages in the queue. curl can also subscribe,
`curl -N --max-time 30 -u ingest:<password>
mqtt://<broker>:1883/skuhus/acme/vasby/pack-03/printer-1/status`, which prints
each message as its topic and payload behind MQTT's length bytes, and exits 28
when the time runs out.

## Discovering agents and devices

The agent answers no discovery request. Each agent announces itself instead:
a keepalive on `skuhus/<project>/<site>/<station>/agent/<instance>/status`
every `status.keepalive_interval`, and at once each time it connects. It names
the station, the instance and the version, and lists every device with its
`device_id`, `device_type`, whether its port is open, and its counters, so a
device's topics follow from it. Nothing is retained, so a new consumer hears of
each agent within one interval, 15 s by default; `skuhus-ingest.agent-status`
holds the recent keepalives for one that was not listening.
