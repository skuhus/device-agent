# Design v2: device agent

Tracking issue: #4, "Implement abstract device agent". First release: 2.0.0.
Postponed to their own tickets: #5 flow control and write pacing, #6 reloading
the configuration on SIGHUP, #7 framing for devices that send no separator.

Issue #4 says "as per the discussions". This document is those discussions,
written down, so that the work can be done by someone who was not in them.

## Reading this document

Every decision carries one marker:

- `[Decided]` - agreed by the maintainer. The source is given.
- `[Proposed]` - raised in the discussion and not confirmed. Do not build on it
  until it is marked Decided.
- `[Open]` - needs a decision. The task that depends on it says so.
- `[Deferred]` - agreed to be outside #4.
- `[Carried over]` - a v1 decision that still holds. Its reasoning is under
  "Carried over from v1", in the section named.

"Maintainer" means Pavel Kim in the #4 design discussion, with the date. A file
reference about v1 is to commit f97c736, before the v2 rename moved the files;
one about v2 is to the tree. v1's design notes, DESIGN.md, were folded into
"Carried over from v1" and removed (T12); they are at f97c736.

## Scope: a transport between a serial port and MQTT

`[Decided]` The agent reads bytes from a serial port and publishes them to MQTT,
and in a later phase writes bytes received from MQTT to the port. It implements
no device protocol. Parsing, interpretation and relaying are done by separate
services that subscribe to what the agent publishes. Source: maintainer,
2026-09-19.

The motivation, as given: move device logic off the station and out of the
agent, and make device protocol errors visible to the services that understand
the protocol, rather than only as log lines on the host.

`[Decided]` Reading ships first, writing second. Source: maintainer, 2026-09-19.

Printers are the main reason for writing; they are "a big chunk of our needs"
(maintainer, 2026-09-19). DIGI scales are served by the pydigi library today.
Whether they move onto this agent depends on the exchange session, which is
deferred; see "Deferred beyond #4".

The v1 read path is already device-neutral. The framer knows a separator, a
maximum frame size and an inter-character timeout
(internal/device/serial/framer.go). The only scanner-specific text in the
device layer is the assert_config warning (internal/device/serial/serial.go:212).
The v1 envelope already carries the whole frame unmodified ("The
whole frame is the payload"). The conversion is therefore mostly removal and a
new topic and message layout, not new reading logic.

## Version: 2.0.0, and 1.0.0 is never used

`[Decided]` The first release of the device agent is 2.0.0. 1.0.0 was the target
of the scanner agent, which was never released under it; the number is skipped
and not reused. Source: maintainer, 2026-09-29.

Builds before it were 0.x, up to 0.3.0. The constant is 2.0.0 from T13 on
(internal/version/version.go). The release workflow releases whenever a merge
to master carries a version that has no tag yet (.github/workflows/release.yml),
so the merge of T13's pull request releases 2.0.0.

`[Decided]` 2.0.0 releases reading and writing together. Source: maintainer,
2026-10-04 ("we need both rx and tx in the release now"). This replaces the
decision of 2026-09-29 that 2.0.0 releases reading and 2.1.0 writing.

`[Decided]` A release's notes start with a link to README.md, "Upgrading", at
the release's tag, above the notes GitHub generates; that section says what a
station and a consumer change for each release. Source: maintainer, 2026-10-05
(#18 Q1). For 2.0.0 it lists what changed from 0.3.0.

`[Decided]` The image on GHCR, `ghcr.io/skuhus/skuhus-device-agent`, is public,
so that a station pulls it without credentials. Source: maintainer, 2026-10-05
(#18 Q2). The 0.3.0 image, `ghcr.io/skuhus/skuhus-device-serial-scanner`,
refuses an anonymous pull, so the new package is expected to start private too,
and an organisation owner makes it public after the release first pushes it.

## Naming

`[Decided]` The repository is skuhus/device-agent, renamed on 2026-09-29. The Go
module path follows it: github.com/skuhus/device-agent, changed by T3 (#8).

`[Carried over]` One name for the binary, the configuration directory, the log
directory, the release assets, the container image and the account inside it
("One name for one thing").

`[Decided]` That name is skuhus-device-agent. Source: maintainer, 2026-09-29.

`[Decided]` The environment variable prefix is SH_DEV_AGENT_. Source:
maintainer, 2026-09-29.

## Topics

`[Decided]` Every device has its own topics. Source: maintainer, 2026-09-29
("The suggested path structure seems ok").

    skuhus/<project>/<site>/<station>/<device>/rx       agent -> subscribers: bytes read from the port
    skuhus/<project>/<site>/<station>/<device>/tx       senders -> agent: bytes to write to the port
    skuhus/<project>/<site>/<station>/<device>/status   agent -> subscribers: device events, tx results

`[Decided]` A channel beside rx and tx carries progress messages, execution
results and agent keepalives, "so everyone could see all available things on
all agents". The working name is status; meta was the alternative. Source:
maintainer, 2026-09-29.

These replace the v1 topics scan, cmd, cmd/result, status and heartbeat, which
had no device segment (internal/transport/mqtt/topics.go). A v1 consumer
receives nothing from a v2 agent, which is one reason the release is 2.0.0.

A consumer takes every device at a station with a single-level wildcard:
`skuhus/<project>/<site>/<station>/+/rx`.

A tx can also reach every device in a broadcast group, on the group's own topic
("Broadcast groups", #35).

`[Decided]` Status at two levels. Source: maintainer, 2026-09-29.

    skuhus/<project>/<site>/<station>/agent/<instance>/status   will, keepalive, counters
    skuhus/<project>/<site>/<station>/<device>/status           device events, tx results

- A will is registered on one topic per connection
  (internal/transport/mqtt/client.go:143), so "this agent is gone" cannot be
  published on each device's topic.
- The instance is in the path because a station-level status collided when two
  agents shared a station: one instance shutting down published a retained
  offline for a station whose other instance was running and scanning
  ("instance_id, not host").
- The segment `agent` is reserved and rejected as a device id, so no device can
  be configured into the agent's topics.

`[Decided]` A device id is now a topic segment, so it follows the topic-segment
rule `[a-z0-9-]+` (internal/config/validate.go:24). v1 allows `[A-Za-z0-9._-]+`
(validate.go:27), which was acceptable while the id only appeared inside
payloads. Source: maintainer, 2026-09-29 (#23 Q1).

`[Decided]` The instance is a topic level too, `agent/<instance>/status`, so it
follows the same rule for the same reason. v1 allows `[A-Za-z0-9._-]{1,64}`,
because there the instance was only the MQTT client id (validate.go:30 and
74-84). internal/wire refuses an instance outside the rule, and T5 checks it in
the configuration. A station that sets an instance outside the rule refuses to
start after the upgrade until the value is changed; one that does not set it
takes the station id, which already follows the rule. Source: maintainer, 2026-09-29 (#11 Q2).

## Message ids and execution results

`[Decided]` Every rx and every tx message carries a UUID. Source: maintainer,
2026-09-29.

- rx: generated by the agent, one per frame, as v1's event_id.
- tx: supplied by the sender.

`[Decided]` For each tx the agent publishes an execution result naming the tx id,
so the sender learns what happened to it. Source: maintainer, 2026-09-29.

`[Decided]` The result goes on the device's status topic, with one of three
states: accepted (received and queued for the port), written (every byte reached
the port), failed (with a reason and, for port errors, the error class v1
already assigns: absent, busy, permission_denied, read_only, disconnected).
Source: maintainer, 2026-09-29 (#23 Q2).

`[Decided]` Every result and every event carries as much information as the
agent can easily provide: at least a machine-readable code and a human-readable
text, and whatever detail is at hand, such as the error class, the byte count or
the number of attempts. Source: maintainer, 2026-09-29 (#23 Q2).

`[Decided]` The tx id is an idempotency key. The agent remembers recent tx ids
and does not write a tx whose id it has already written; it publishes a result
saying so instead. A sender that saw no result can then resend safely.
Source: maintainer, 2026-09-29 (#23 Q15).

`[Decided]` The tx id must be a UUID, not arbitrary data. A tx carrying anything
else fails with a code saying why. Source: maintainer, 2026-09-29 (#23 Q15).

`[Decided]` Any RFC 9562 UUID version is accepted, not only version 4.
Uniqueness is all idempotency needs, and a sender using version 7 gets ids that
sort by time at no cost to the agent. The agent's own rx ids stay version 4.
Source: maintainer, 2026-09-29 (#23 Q15a).

`[Deferred]` Restricting tx ids to version 4, as a possible refactoring: #27.
Source: maintainer, 2026-09-29 (#23 Q15a).

`[Decided]` The tx carries a sender field, set by the sender. Several senders
may write to one device and there is no session, so the sender field is the
only record of who wrote what when an interleaving has to be reconstructed. For
now it only indicates the sender, and nothing trusts it. Source: maintainer, 2026-09-29 (#23 Q3).

`[Deferred]` Using the sender field as a path to authenticating senders.
Source: maintainer, 2026-09-29 (#23 Q3).

`[Decided]` The id travels in the JSON payload, not as MQTT 5 correlation data.
Correlation data works on RabbitMQ 4.x (docs/spikes/m0-mqtt5.md:100), but a
payload field is visible in every log and to every consumer, and does not depend
on the broker supporting the property. Source: maintainer, 2026-09-29 (#23 Q4).

`[Deferred]` Supporting MQTT 5 correlation data as well. Source: maintainer, 2026-09-29 (#23 Q4).

## Reading: rx

`[Decided]` Each device in the configuration defines its port, its serial
parameters and its separator. Source: maintainer, 2026-09-19.

`[Carried over]` Framing rules, each with the measurement behind it under
"Carried over from v1":

- max_frame_bytes counts the payload and excludes the separator
  ("max_frame_bytes is the payload size, excluding the separator").
- After any discard the framer drops bytes up to the next separator ("Any
  discard resynchronises to the next separator").
- Resynchronisation also ends when the device is silent for one inter-character
  timeout ("Choosing inter_char_timeout").
- The reopen backoff resets after a session that stayed up, not after an open
  that succeeded ("The backoff resets on session duration, not on a successful
  open").

`[Decided]` Stray bytes are a configuration error or a misbehaving device. They
are fixed in the configuration or by IT; the agent reports them and does not
compensate. Source: maintainer, 2026-09-29.

`[Decided]` Every discard is a warning in the log and is counted in the metrics.
Source: maintainer, 2026-09-29. v1 logs oversize and timeout discards at WARN and
resync and empty-frame discards at DEBUG, and counts none of them
(internal/device/serial/serial.go:328-341).

`[Decided]` The failure that matters most is an unmatched separator: the
configured separator never appears in what the device sends, so no reading is
ever emitted. It has two causes. The separator is misconfigured, such as `\n\r`
for a device that sends CRLF, or it is missing from the data, as from a scanner
set up without a suffix. Source: maintainer, 2026-09-29 and 2026-10-01. The
Symbol 05e0:1701 capture shows the second: no suffix, one inter_char_timeout
discard per scan, and no frames (docs/scanners/symbol-05e0-1701.md:32).

A separator configured as only part of what the device sends, such as CR for a
device that sends CRLF, is a different failure. Frames still come out, and the
stray byte is discarded or prepended to the next reading ("A CR/CRLF
mismatch is the one wrong separator that is not loud").

`[Decided]` That failure is detected upstream from the counters, not in the
agent: for one device, rx bytes rising, rx frames flat, timeout discards rising.
Source: maintainer, 2026-09-29 (#23 Q5).

`[Deferred]` Framing for devices that send no separator, ended by a timeout, a
size, or both: #7. Source: maintainer, 2026-09-29, as no device in use needs it.
v1 discards on both triggers because publishing the tail of a frame produced
plausible, wrong readings when a CRLF device was configured as CR ("A
CR/CRLF mismatch is the one wrong separator that is not loud"). #7 records the
constraints: chosen per device, never the default where a separator is
configured, and the size semantics settled with a device on the bench.

`[Carried over]` rx is published at QoS 1 with a message expiry, through a
bounded buffer, and is never retried or replayed ("Scans are
perishable" and "A failed publish is not retried").

`[Decided]` The message expiry becomes a per-device setting. v1 has one
delivery.scan_ttl for the whole process (internal/config/config.go:114); a
scanner and a scale on one station need not give their readings the same
lifetime. Source: maintainer, 2026-09-29 (#23 Q6).

`[Decided]` A device's message expiry is reported in its status messages, as a
troubleshooting aid. Source: maintainer, 2026-09-29 (#23 Q6).

`[Decided]` A device may declare a `device_type` string, published with every
message about that device, rx and status alike. It tells the services behind the
broker which physical device this is, not only which response format to expect:
it bridges the hardware IT connected to a station and how the station is
configured. The agent carries it and does not interpret it. Source: maintainer, 2026-09-29 (#23 Q7). It
replaces the proposed `model`.

## Writing: tx

`[Decided]` The agent is a transport for writes as it is for reads, and does not
implement device protocols. Source: maintainer, 2026-09-29.

`[Decided]` A write succeeds when every byte of it was written to the port, as
the operating system reports it. Success says nothing about the device. Source:
maintainer, 2026-09-29.

On the bench printer, an Epson TM-T20III behind a PL2303 adapter at 9600 baud
on macOS, the adapter's driver takes a job in 16 KB blocks. write(2) accepted
16 KB at once, then blocked for 16.3 s, the time that block takes on the line,
before it took the next; Drain returned as soon as write(2) did
(docs/printers/epson-tm-t20iii.md). So `written` can come up to about 17 s
before the device has the last bytes, and the agent does not use Drain (T14).

`[Decided]` Several senders may write to one device, and the agent does not
prevent it. Source: maintainer, 2026-09-29.

`[Decided]` There is no session. What the device sends back appears on rx like
any other reading, with no link to the tx that caused it. Source: maintainer,
2026-09-29.

`[Decided]` A tx is written to the port contiguously. Senders are not ordered
against each other, but the bytes of two tx messages are never interleaved on
the wire: one writer per port. Source: maintainer, 2026-09-29 (#23 Q16).

`[Decided]` There is no minimum gap between writes, and no setting to prioritise
reading or writing. A serial line is full-duplex and the agent reads on its own
goroutine throughout, so reading continues while a tx is written, and a device's
response reaches rx without the writer pausing for it. Source: maintainer, 2026-09-29 (#23 Q16a).

`[Decided]` A tx that cannot be written is retried up to a configured number of
times, then fails with a result that carries the port's error class and the
attempts made. Source: maintainer, 2026-09-29 (#23 Q17). This replaces the proposal to fail at once.

`[Decided]` Retries cover opening the port only: a tx that finds its port closed,
or whose attempt to open it fails, is retried. Once writing has started, a
failure is not retried, whatever was written, because a retry could put data on
the wire twice and a printer would print it; the result reports how many bytes
were written. The retry count and the interval between attempts are device
settings, and no attempt is made once the tx's message expiry has passed.
Source: maintainer, 2026-09-29 (#23 Q17a).

The settings are `tx_open_attempts`, 3 by default, and `tx_open_interval`, 1 s
by default (T14). The device's reader keeps owning the port. A tx that finds it
closed asks the reader to try opening it now, rather than after its backoff,
and waits up to the interval for it to open; that is one attempt. The expiry is
the tx's MQTT message expiry as it arrived; a tx sent without one is written
whenever its turn comes.

A tx is written `tx_chunk_bytes` at a time, 1024 by default, with the port's
lock held for one piece (internal/device/serial/serial.go, sharedPort). The
reader's close waits for the piece being written, not for the whole tx, and a
stopping agent stops a tx between pieces. At 9600 baud 1024 bytes is about a
second on the line once the operating system's buffer is full.

Tx results go out through the device's event queue, so they wait for the
connection as its events do (#13 Q1), in order. Unlike an event, a result is
never dropped to make room: a sender that hears nothing resends, and a printer
prints the job twice. Results cannot pile up during an outage, because no tx
arrives without the connection.

`[Decided]` A tx the agent stops before writing fails as `agent_stopping`,
with the bytes written. A tx being written when the agent starts to stop goes
on for at most the drain, `delivery.drain_timeout` (5 s by default, a key since
#30), with its port still open, and then stops after
the chunk in hand; a queued one, or one that arrives while the agent stops, is
not started and gets `bytes_written` 0. Source: maintainer, 2026-10-04
(#19 Q1).

A tx published while the agent is reconnecting must not be lost silently. v1
connects with clean start and session expiry 0
(internal/transport/mqtt/client.go:107-108), so no session survives a
disconnect. While the agent reconnects its tx subscription does not exist; a tx
published in that window has no subscriber, the broker drops it, and no result
is ever published.

`[Decided]` The contract states that a sender that receives no result within a
stated time treats the tx as not written. Source: maintainer, 2026-09-29 (#23 Q18).

`[Decided]` Senders set a message expiry on every tx, so that a tx that waited too
long is dropped rather than written late. M0 measured the broker honouring it: a
2 s message expired during a 6 s absence while a 300 s one was delivered
(docs/spikes/m0-mqtt5.md:108). Source: maintainer, 2026-09-29 (#23 Q19).

`[Decided]` rx and tx share one connection, which starts clean and keeps no
session, as v1's does. Nothing is resubmitted after a reconnect. A tx published
while the agent is disconnected is lost, and the sender learns it from the
missing result. Source: maintainer, 2026-09-29 (#23 Q19a).

A tx that arrives while the agent's intake is full is lost the same way: it is
recorded in the log at ERROR with its data, and gets no result. paho hands
messages over one after another and must not wait, or every acknowledgement
behind the tx waits too (cmd/skuhus-device-agent/run.go, onMessage). The intake
is `delivery.tx_intake_size`, 256 by default; the core takes each tx at once,
so it fills only when senders flood a station.

`[Deferred]` Keeping tx through reconnects: #20. Source: maintainer, 2026-09-29 (#23 Q19a). It needs a
second connection for tx, with its own client id and a session kept across
reconnects and restarts. Session settings belong to a connection, not to a
subscription (autopaho's ClientConfig), and a session on the rx connection
would change rx: when a session resumes, paho resends every
unacknowledged publish in its store (paho/session/state/state.go:208-210 in
paho.golang v0.23.0), so a scan the agent had already recorded as failed would
be delivered late. With clean start there is never a session to resume, and
paho clears its store instead (state.go:171-173).

`[Decided]` The serial library stays go.bug.st/serial. Source: maintainer,
2026-09-29. It supports data bits, parity and stop bits (its `Mode`), keeps the
error code when a write fails, and reports a read timeout as zero bytes rather
than end of file; v1's read path is tested against it with a real scanner.

`[Deferred]` Flow control and pacing: #5. Source: maintainer, 2026-09-29.
go.bug.st/serial always switches flow control off (serial_unix.go:242 and
422-423 in v1.8.0), and no alternative library does better; #5 records the
evaluation and the proposed replacement on `*os.File` and golang.org/x/sys/unix.
Printers on `/dev/usb/lpN` or raw TCP use no termios and are not affected.

`[Decided]` The write path protects itself from two properties of the library.
Its `Write` takes no lock and does not check that the port is open, so a write
racing a close reaches whatever descriptor now has that number; and it makes one
write(2) call without continuing after a partial write (serial_unix.go:112-118).
The agent's port type holds one lock across `Write` and `Close`, and loops until
every byte is written. Source: maintainer, 2026-09-29 (#23 Q20).

## Broadcast groups

`[Decided]` A device can be in broadcast groups at three scopes, its project,
its site and its station, and one tx on a group's topic reaches every device in
the group. Each device declares its groups, one list per scope. Source:
maintainer, 2026-10-06 (#35 Q1, Q2, Q1b).

```yaml
devices:
  - id: scales-a
    broadcast_groups:
      project: [scales]
      site: [scales]
      station: []
```

| Scope | Topic | Levels |
|---|---|---|
| project | `skuhus/<project>/group/<group>/tx` | 5 |
| site | `skuhus/<project>/<site>/group/<group>/tx` | 6 |
| station | `skuhus/<project>/<site>/<station>/group/<group>/tx` | 7 |

A group name is a topic level, so it follows `[a-z0-9-]+`, as a device id does
("Topics"). The site topic has the shape of a device's tx topic, so `group` is
reserved as a station id, as `agent` is reserved as a device id. No other topic
has the project topic's or the station topic's number of levels (#35 Q1b).

`[Decided]` A group is not a `device_type`, which does not identify a model.
What describes a device, a `device_info` with its model, description, serial
number and part number, is decided on #36. Source: maintainer, 2026-10-06
(#35 Q1, Q1a).

The agent subscribes to a group's topic once, however many of its devices are in
the group, and hands a tx on it to each of them (cmd/skuhus-device-agent/run.go,
buildDevices; internal/core/tx.go, admitTx). Tx ids are kept per device
(internal/core/pipeline.go:28-33). Each device therefore takes the tx once and
publishes its own results on its own status topic, all with the same `tx_id`. A
resend is answered per device, as "The tx contract" describes, and so is the
same tx reaching a device through two of its groups. The log records each tx on
a group's topic with the devices it went to, and each device's admission with
the topic it came on.

`[Decided]` Each device's entry in the keepalive lists every group the device
is in, with its scope, and every tx topic that reaches the device: its own and
each group's. A topic is reported as the filter that went into the SUBSCRIBE
packet, with the broker's SUBACK answer to it, both recorded when the agent
subscribes. The value used to subscribe is the value reported, so the keepalive
cannot drift from what was subscribed. Source: maintainer, 2026-10-06 (#35 Q3,
Q3a, Q3b). The field is `tx_topics` ("Agent keepalive"). The integration test
compares it with the SUBSCRIBE and SUBACK packets that went over the wire
(test/integration, TestBroadcastGroupTxReachesEveryDeviceInTheGroup).

MQTT gives a client no way to read the broker's bindings. The other option was
reading them from RabbitMQ's management API, and it was not chosen: it needs an
HTTP credential with the `management` tag on every station, and works with
RabbitMQ only (#35 Q3a).

A station's agent needs read permission on its project and site group topics
before it subscribes to them; dev/rabbitmq/definitions.json has an example.
Measured on 2026-10-06 against RabbitMQ 4.3.5 (#35):
- mosquitto_sub 2.1.2, subscribing as a station: with one topic in a SUBSCRIBE
  refused, the broker closed the connection after the SUBACK, and the client
  received 0 of 10 tx sent to its own device. With every topic permitted, it
  received 10 of 10.
- The agent, with a site group its station may not read: it connected 549
  times in 8 s. Each time the broker closed the connection over the refused
  subscription (`subscribe_error`), the agent logged that subscribing failed,
  and it reconnected at once, since the first attempt after a lost connection
  does not wait ("Reconnecting to the broker"). With the permission in place it
  connected once.

A group subscription the broker refuses therefore takes the whole agent off the
broker, and makes it reconnect without a pause; the permissions come before the
configuration. The agent logs a refusal that the broker answers without closing
the connection as `subscription refused; no tx will arrive on this topic`
(internal/transport/mqtt/client.go, subscribe).

## Status channel

`[Decided]` The agent publishes keepalives on the status channel, so that every
agent and every device can be seen from outside. Source: maintainer, 2026-09-29.

`[Decided]` Device problems are reported through the status channel as well as
the log, "not only via logs on the host computer". Source: maintainer,
2026-09-19.

`[Decided]` Device events on the device status topic: port opened, port closed,
port lost with its error class, discard with its reason and byte count, tx
result. The list is a minimum: anything that keeps consumers reasonably informed
about a device or the agent belongs there too. Source: maintainer, 2026-09-29 (#23 Q8).

`[Decided]` Counters in the agent keepalive, per device: rx frames, rx bytes,
discards by reason, failed opens by error class, publish failures, tx written,
tx failed, buffer depth. The v1 heartbeat carries some of these
(internal/event/heartbeat.go:24-38). They are pushed over MQTT rather than
scraped over HTTP: stations are not generally reachable for scraping, and
publishing keeps the numbers off the host, which is the aim of the design.
Source: maintainer, 2026-09-29 (#23 Q9).

`[Deferred]` The same counters on an HTTP endpoint for scraping as well: #26.
Source: maintainer, 2026-09-29 (#23 Q9).

`[Carried over]` Liveness comes from the keepalive, not from the retained
status. RabbitMQ delivers a will without retaining it, and does not replicate
retained messages across cluster nodes (docs/spikes/m0-mqtt5.md:102-106). A
consumer relying on the retained status sees a dead agent as online.

## Message formats

`[Decided]` The formats in this section, as reviewed in #11. Source: maintainer, 2026-09-29
(#11 Q1-Q9 and Q6a). The earlier decisions they build on are cited where they
are used.

`[Decided]` Names may change where the change improves them. Source:
maintainer, 2026-09-29 (#11 Q9). Changed since the first draft: `expiry_s` to
`message_expiry_s`, which says whose expiry; `open_failed` and `discard` to
`port_open_failed` and `bytes_discarded`, past tense like the other event codes;
and `attempts` to `open_attempts`, which says what was attempted.

Every message is one JSON object in UTF-8, published with the content type
`application/json`. Field names are snake_case and timestamps are RFC 3339 in
UTC with milliseconds, both as in v1 (internal/event/scan.go). internal/wire
builds every message below, and its tests hold this section and the code to
each other: each example is what the code builds, value for value, and each
code table lists exactly the codes the code defines.

### Fields in every message the agent publishes

| Field | Type | Null | Meaning |
|---|---|---|---|
| schema | integer | no | 2. v1's messages are schema 1 (internal/event/scan.go); consumers switch on it. |
| kind | string | no | `rx`, `event`, `tx_result`, `keepalive` or `offline`. Each status topic carries two kinds. |
| id | string | no | A UUID, version 4, new for every message. At QoS 1 a message can arrive twice; a consumer that has seen the id drops the copy. On rx it is the UUID every rx message carries ("Message ids and execution results"). |
| project, site, station | string | no | The station, as in the topic. |
| instance_id | string | no | The agent instance: the MQTT client id, and the `<instance>` level of the agent's topics. |
| agent_version | string | no | |
| agent_ts | string | no | When the agent built the message, by the host clock, which nothing vouches for ("Carried over from v1"). |

The identity repeats the topic because a message copied into a log, a ticket or
a database row loses its topic (v1's reason, internal/event/status.go).

A message about one device also carries:

| Field | Type | Null | Meaning |
|---|---|---|---|
| device_id | string | no | The device's topic level. |
| device_type | string | yes | The configured device_type (#23 Q7), null when none is configured. The agent does not interpret it. |

### rx

On `<device>/rx`, one message per frame.

```json rx
{
  "schema": 2,
  "kind": "rx",
  "id": "5b7b4f6e-2f0a-4c1e-9d3a-8f6e1c2b7a90",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "device_id": "scanner-1",
  "device_type": "symbol-05e0-1701",
  "seq": 1042,
  "raw_b64": "NzMxMDQyNTAxMjM0NQ==",
  "text": "7310425012345",
  "text_valid": true
}
```

| Field | Type | Null | Meaning |
|---|---|---|---|
| seq | integer | no | Counts this device's frames from 1 at process start. A gap between two received values means frames were lost between them. It does not survive a restart and is not a dedup key; `id` is. |
| raw_b64 | string | no | The whole frame, separator excluded, in padded standard base64 (RFC 4648). Nothing is stripped ("Carried over from v1"). |
| text | string | yes | The frame when it is valid UTF-8; null otherwise, rather than a lossy rendering. |
| text_valid | boolean | no | Whether `text` is set. |

v1's `symbology`, which was always null, is dropped.

### Device events

On `<device>/status`, with `kind` `event`.

```json event
{
  "schema": 2,
  "kind": "event",
  "id": "c3a1e2d4-5f60-4b7a-8c9d-0e1f2a3b4c5d",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "device_id": "scanner-1",
  "device_type": "symbol-05e0-1701",
  "device_open": false,
  "message_expiry_s": 30,
  "code": "port_lost",
  "text": "port lost: read /dev/serial/by-id/usb-Symbol_Technologies-if00: input/output error (disconnected)",
  "detail": {
    "error": "read /dev/serial/by-id/usb-Symbol_Technologies-if00: input/output error",
    "error_class": "disconnected",
    "path": "/dev/serial/by-id/usb-Symbol_Technologies-if00"
  }
}
```

| Field | Type | Null | Meaning |
|---|---|---|---|
| device_open | boolean | no | Whether the agent holds the port open after this event: what the agent knows, not what the hardware does ("Carried over from v1"). |
| message_expiry_s | integer | no | The device's MQTT message expiry in seconds (#23 Q6). |
| code | string | no | One of the codes below. |
| text | string | no | A sentence for a person. Nothing should parse it. |
| detail | object | no | Keys that depend on the code, below; `{}` for a code with none. |

#### Event codes

| Code | Detail keys |
|---|---|
| `port_opened` | `path` |
| `port_closed` | `path` |
| `port_lost` | `path`, `error_class`, `error` |
| `port_open_failed` | `path`, `error_class`, `error` |
| `bytes_discarded` | `reason`, `bytes` |

The list is a minimum (#23 Q8); codes are added, never renamed. `error_class` is
one of `absent`, `busy`, `permission_denied`, `read_only`, `disconnected`,
`port_error` and `unknown`, the classes v1 assigns
(internal/device/serial/serial.go, classify; internal/wire/messages.go,
ErrorClasses). `error` is the operating system's message. `reason` is one of
`oversize`, `inter_char_timeout`, `resync` and `empty_frame`, the reasons the
framer gives (internal/wire/messages.go, DiscardReasons), and `bytes` is how
many bytes were discarded. Each class and each reason has its counter in the
keepalive, under the same name.

`[Decided]` `port_open_failed` is published on every attempt to open the port,
which keeps the rule simple. Source: maintainer, 2026-09-29 (#11 Q8). A scale
unplugged overnight, retried every 30 s at most (v1's backoff limit,
internal/device/serial/serial.go:27, and the default `reopen_backoff.max`),
publishes about 1,440 of them in 12 hours.

A device's events are published in the order they happened, with `agent_ts`
the time they happened.

`[Decided]` An event that happens while the broker connection is down waits,
and is published once the connection is up. Each device keeps only its most
recent events, so that a long outage does not end in a flood. Source:
maintainer, 2026-10-01 (#13 Q1). How many is `status.event_buffer_size`, 64 by
default; when it is full the oldest is dropped and logged. An event that waited
longer than the device's message expiry is dropped as well, and one within it
is published with what remains of its expiry. The keepalive counts every event,
published or not.

`[Decided]` Readings do not wait. A reading while the connection is down fails
at once and is recorded in the log file with its payload, as before ("Scans
are perishable"): a reading that arrives late can make a consumer act on a scan
the operator has already repeated. Source: maintainer, 2026-10-02 (#13 Q2).

The port opens before the broker connection on every start, so `port_opened` is
one of the events that waits (measured in #13).

### tx

On `<device>/tx`, published by senders. The agent subscribes to it (PLAN-V2.md,
T14).

```json tx
{
  "schema": 2,
  "id": "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b",
  "sender": "label-service",
  "raw_b64": "XlhBXkZEU0tVLTEwNDJeRlNeWFo="
}
```

| Field | Type | Meaning |
|---|---|---|
| schema | integer | 2. |
| id | string | A UUID of any RFC 9562 version (#23 Q15, Q15a), and the idempotency key. |
| sender | string | Who sent it, not empty. Recorded; nothing trusts it (#23 Q3). |
| raw_b64 | string | The bytes to write, in padded standard base64. |

All four are required and no other field is accepted, so a mistake in a sender
gets a failed result instead of being ignored. Source: maintainer, 2026-09-29 (#11 Q3).
Senders set an MQTT message expiry on every tx (#23 Q19).

### tx results

On `<device>/status`, with `kind` `tx_result`. A tx the agent can read gets
`accepted`, then `written` or `failed`. A tx it cannot read, or whose id is not a
UUID, gets `failed` alone. A tx whose id belongs to a tx still queued or being
written gets `rejected` alone, and the earlier one carries on. A tx whose id
was written recently gets `already_written` alone, and is not written again
(#11 Q6a, T16).

```json tx_result
{
  "schema": 2,
  "kind": "tx_result",
  "id": "7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "device_id": "printer-1",
  "device_type": "zebra-zt410",
  "device_open": false,
  "message_expiry_s": 30,
  "tx_id": "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b",
  "sender": "label-service",
  "state": "failed",
  "code": "port_unavailable",
  "text": "the port could not be opened in 3 attempts: open /dev/serial/by-id/usb-Zebra_ZT410-if00: device or resource busy (busy)",
  "detail": {
    "error": "open /dev/serial/by-id/usb-Zebra_ZT410-if00: device or resource busy",
    "error_class": "busy",
    "open_attempts": 3
  }
}
```

| Field | Type | Null | Meaning |
|---|---|---|---|
| device_open | boolean | no | As in device events. |
| message_expiry_s | integer | no | As in device events. |
| tx_id | string | yes | The tx's id; null when the tx could not be read. |
| sender | string | yes | The tx's sender; null when the tx could not be read. |
| state | string | no | `accepted`, `written` or `failed` (#23 Q2), or `rejected` (#11 Q6). |
| code | string | no | One of the codes below. |
| text | string | no | A sentence for a person, with the cause when there is one. |
| detail | object | no | Keys that depend on the code, below; `{}` for a code with none. |

#### Tx result codes

| State | Code | Detail keys | When |
|---|---|---|---|
| `accepted` | `accepted` | | Received and queued for the port. |
| `written` | `written` | `bytes_written`, `open_attempts` | Every byte reached the port. |
| `written` | `already_written` | `written_at` | A tx with this id was written before, so this one was not (#23 Q15). The agent remembers the ids of the last `tx_remembered_ids` tx written to each device, 1024 by default, in memory, and a restart forgets them (#11 Q6a, T16, #30). |
| `rejected` | `in_progress` | `stage`, `since`, `bytes_written` | A tx with this id is queued or being written, so this one is not taken (#11 Q6). |
| `failed` | `invalid_message` | `error` | Not JSON, `schema` is not 2, a field is missing or unknown, or `raw_b64` is not base64. |
| `failed` | `invalid_id` | | `id` is not a UUID. |
| `failed` | `port_unavailable` | `error_class`, `error`, `open_attempts` | The port could not be opened in the configured number of attempts (#23 Q17). |
| `failed` | `write_failed` | `error_class`, `error`, `bytes_written`, `open_attempts` | Writing started and failed. It is not retried, and `bytes_written` says how far it got (#23 Q17a). |
| `failed` | `expired` | `open_attempts` | The tx's message expiry passed before an attempt could start (#23 Q17a). |
| `failed` | `agent_stopping` | `bytes_written` | The agent stopped before the tx was written, or part way through (#19 Q1). |

`error_class` and `error` are as in device events. `bytes_written` is how many
bytes reached the port, for `in_progress` so far. `open_attempts` counts the
attempts to open the port for this tx, 0 when it was open. `stage` is `queued`
or `writing`, and `since` is when the earlier tx entered that stage.
`written_at` is when the earlier tx was written.

### The tx contract

- `[Decided]` The contract names no time. Each sender sets its own timeouts,
  for `accepted` and, after it, for `written` or `failed`, and treats a tx with
  no result by then as not written (#23 Q18). A tx published while the agent is
  disconnected is lost and never gets a result (#23 Q19a). Source: maintainer, 2026-09-29
  (#11 Q4, Q5).
- `[Decided]` Sending the same tx again, with the same id, is how a sender asks
  where it stands, until an enquiry of its own exists (#28). While a tx with
  that id is queued or being written, the resend is rejected with
  `in_progress`, which says where the earlier tx stands and since when, and
  nothing is queued twice. Source: maintainer, 2026-09-29 (#11 Q5, Q6).
- `[Decided]` The agent keeps no record of ids beyond what the running agent
  holds: the txs queued or being written, and a bounded set of recently written
  ids. A restart forgets both. A resend after `written` gets `already_written`
  while the id is in the set. Source: maintainer, 2026-09-29 (#11 Q6, Q6a).
  Only a written tx joins the set: one that failed, part way or not at all, is
  written again if it is sent again, since its sender was told what became of
  it (T16).
- `accepted` says the tx reached the agent. Only `written` says the bytes
  reached the port, and it says nothing about the device ("Writing: tx").

A resend while the earlier tx is being written:

```json tx_result_in_progress
{
  "schema": 2,
  "kind": "tx_result",
  "id": "2d3c4b5a-6978-4e5f-8a1b-0c9d8e7f6a5b",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "device_id": "printer-1",
  "device_type": "zebra-zt410",
  "device_open": true,
  "message_expiry_s": 30,
  "tx_id": "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b",
  "sender": "label-service",
  "state": "rejected",
  "code": "in_progress",
  "text": "a tx with this id is being written since 2026-09-29T07:58:30.123Z, 61440 bytes so far; this one was not taken",
  "detail": {
    "bytes_written": 61440,
    "since": "2026-09-29T07:58:30.123Z",
    "stage": "writing"
  }
}
```

### Agent keepalive

On `agent/<instance>/status`, with `kind` `keepalive`.

`[Decided]` Every 15 seconds by default, v1's interval
(internal/transport/mqtt/client.go:23), and a consumer treats the agent as gone
after 3 missed keepalives, 45 seconds by default. Both are configurable, and the
keepalive carries the result, so a consumer applies the agent's configuration
rather than a number of its own. Source: maintainer, 2026-09-29 (#11 Q7).

```json keepalive
{
  "schema": 2,
  "kind": "keepalive",
  "id": "9f8e7d6c-5b4a-4392-8170-6f5e4d3c2b1a",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "uptime_s": 3600,
  "interval_s": 15,
  "gone_after_s": 45,
  "devices": [
    {
      "device_id": "scanner-1",
      "device_type": "symbol-05e0-1701",
      "device_open": true,
      "message_expiry_s": 30,
      "tx_topics": [
        {
          "topic": "skuhus/acme/vasby/pack-03/scanner-1/tx",
          "scope": "device",
          "group": null,
          "suback": 1
        }
      ],
      "rx_frames": 1042,
      "rx_bytes": 15656,
      "discards": {
        "oversize": 0,
        "inter_char_timeout": 2,
        "resync": 0,
        "empty_frame": 0
      },
      "failed_opens": {
        "absent": 0,
        "busy": 0,
        "permission_denied": 0,
        "read_only": 0,
        "disconnected": 0,
        "port_error": 0,
        "unknown": 0
      },
      "publish_failures": 0,
      "tx_written": 0,
      "tx_failed": 0,
      "buffer_depth": 0
    },
    {
      "device_id": "printer-1",
      "device_type": "zebra-zt410",
      "device_open": false,
      "message_expiry_s": 30,
      "tx_topics": [
        {
          "topic": "skuhus/acme/vasby/pack-03/printer-1/tx",
          "scope": "device",
          "group": null,
          "suback": 1
        },
        {
          "topic": "skuhus/acme/vasby/group/printers/tx",
          "scope": "site",
          "group": "printers",
          "suback": 1
        }
      ],
      "rx_frames": 0,
      "rx_bytes": 0,
      "discards": {
        "oversize": 0,
        "inter_char_timeout": 0,
        "resync": 0,
        "empty_frame": 0
      },
      "failed_opens": {
        "absent": 0,
        "busy": 3,
        "permission_denied": 0,
        "read_only": 0,
        "disconnected": 0,
        "port_error": 0,
        "unknown": 0
      },
      "publish_failures": 0,
      "tx_written": 0,
      "tx_failed": 1,
      "buffer_depth": 0
    }
  ]
}
```

| Field | Type | Null | Meaning |
|---|---|---|---|
| uptime_s | integer | no | Seconds since the process started. A restart loop shows as a count that keeps returning to zero. |
| interval_s | integer | no | Seconds until the next keepalive. |
| gone_after_s | integer | no | Seconds without a keepalive after which a consumer treats the agent as gone: the configured number of missed intervals. |
| devices | array | no | One entry per configured device, in configuration order; `[]` with none. |

Each entry carries `device_id`, `device_type`, `device_open` and `message_expiry_s` as a
device event does. `tx_topics` lists every topic that reaches the device's tx,
its own first, then its broadcast groups' in the order project, site, station
("Broadcast groups", #35 Q3, Q3a, Q3b):

| Field | Type | Null | Meaning |
|---|---|---|---|
| topic | string | no | The topic filter as it went into the SUBSCRIBE packet. |
| scope | string | no | `device` for the device's own topic; `project`, `site` or `station` for a broadcast group's. |
| group | string | yes | The broadcast group's name; null for the device's own topic. |
| suback | integer | yes | The broker's SUBACK reason code for the topic on the current connection: 0 to 2 grant that QoS, 128 and above refuse it. Null until the broker has answered on this connection. |

The entry also carries the device's counters since the process started, the
list in "Status channel" (#23 Q9):

| Counter | Counts |
|---|---|
| rx_frames | Frames taken for publishing, whatever became of them: the last rx `seq`. |
| rx_bytes | Every byte read from the port, separators and discarded bytes included, so that it rises while `rx_frames` stays flat when nothing is framed (#23 Q5). |
| discards | Discards by reason, one per `bytes_discarded` event. |
| failed_opens | Failed attempts to open the port by error class, one per `port_open_failed` event. |
| publish_failures | Readings the broker did not take: failed, or dropped at the shutdown drain. Each has a record in the log file. |
| tx_written, tx_failed | Tx results (PLAN-V2.md, T14). |
| buffer_depth | Frames waiting to be published now. |

Every reason and every class is present, at 0 when nothing happened, so a
consumer can difference two keepalives without handling a missing key. An
unmatched separator ("Reading: rx") shows as `discards.inter_char_timeout` and
`rx_bytes` rising while `rx_frames` stays flat.

Besides every interval, a keepalive goes out as soon as the broker connection
comes up. The first one does not wait an interval, and a consumer that saw a
will learns within a moment that the agent is back. It goes once the broker has
answered the agent's subscriptions, or subscribing has failed, so that its
`tx_topics` carry the answers.

### Agent offline

On `agent/<instance>/status`, with `kind` `offline`. The agent publishes it
itself when it stops cleanly (`reason` `shutdown`), because a clean disconnect
discards the will. It is also registered as the will (`reason` `will`), which
the broker publishes when the connection is lost without a disconnect.

```json offline
{
  "schema": 2,
  "kind": "offline",
  "id": "1a2b3c4d-5e6f-4a0b-9c8d-7e6f5a4b3c2d",
  "project": "acme",
  "site": "vasby",
  "station": "pack-03",
  "instance_id": "pack-03",
  "agent_version": "2.0.0",
  "agent_ts": "2026-09-29T08:00:00.123Z",
  "reason": "will"
}
```

In a will, `agent_ts` is when the lost connection was made, not when it died:
the agent composes the will for each connection, and the broker sends the one
it was given. The time of death is when the message arrives, which only the
consumer knows.

A will says that a connection was lost, not that the agent stopped. Restarted,
Mosquitto 2.1.2 published the will of the connection it dropped, and the agent
reconnected a second later and went on publishing (#12); RabbitMQ was not
restarted for that measurement. Liveness comes from the keepalive ("Status
channel").

### Publishing

| Message | Topic | QoS | Retained | Message expiry |
|---|---|---|---|---|
| rx | `<device>/rx` | 1 | no | the device's (#23 Q6) |
| event, tx result | `<device>/status` | 1 | no | the device's |
| keepalive | `agent/<instance>/status` | 0 | no | `gone_after_s`: a keepalive older than that says nothing true. v1 used four intervals (internal/transport/mqtt/client.go:24-27). |
| offline | `agent/<instance>/status` | 1 | no | none |
| tx | `<device>/tx` | 1, by senders | no | set by the sender (#23 Q19) |

`[Decided]` Nothing is retained. v1 retains its status and its will
(internal/transport/mqtt/client.go:141-149, 182-192), and three measurements
leave that of little use on RabbitMQ: a will is delivered but not retained, so
the last retained status of a dead agent reads online; a retained message is
not visible through another cluster node; and on 4.3.5 a retained message
reaches only a subscription naming its exact topic, not a wildcard one such as
`+/status` (docs/spikes/m0-mqtt5.md). A consumer learns an agent's state, and
every device's, from the next keepalive instead, within one interval. Source: maintainer, 2026-09-29
(#11 Q1).

A consumer at a station subscribes to:

- `skuhus/<project>/<site>/<station>/+/rx` for every device's readings;
- `skuhus/<project>/<site>/<station>/+/status` for every device's events and tx
  results. It does not match the agents' topics, which are one level deeper,
  and no device can be called `agent`;
- `skuhus/<project>/<site>/<station>/agent/+/status` for every agent's
  keepalives and offline messages.

## Reconnecting to the broker

`[Decided]` While the broker cannot be reached, the agent tries it every second
by default, and the configuration can set another interval. An exponential
backoff can be turned on and off, and is off by default. Source: maintainer,
2026-10-04 (#13 Q3). The interval is `broker.reconnect_interval`; the backoff
is `broker.reconnect_backoff`, with `enabled`, `max` and `jitter`.

The question came from a measurement with the Symbol 05e0:1701. The agent then
used the specification's `connect_backoff`: 1 s doubling to a 60 s cap, with
30% jitter (device-agent-spec.md:303). After a 98 s outage it reconnected 31 s
after the broker was back. A reading made in that time failed, and the 8 events
of an unplug during the outage were dropped as older than their 30 s message
expiry. With a 1 s interval, it reconnected 0.85 s after the broker was back.

The first attempt does not wait, at start and after a lost connection.
autopaho asks for a wait before that attempt as well (autopaho/backoff.go,
Backoff). The agent used to answer with its full first delay there, and took
1.06 s to connect at every start.

With the backoff off, every wait is the interval exactly, with no jitter. With
it on, the wait doubles from the interval up to `max`, and each wait is spread
by `jitter`, a fraction either way, so that stations that lost the broker
together do not retry in step (internal/backoff, Policy.Wait, which the
device reopen uses as well).

Each attempt, from dialling to the broker's CONNACK, is bounded by
`broker.connect_timeout`, 10 s by default, autopaho's own default, which the
agent used before it was a key (autopaho/net.go; #30). The same bound applies
to the broker's answer to the tx subscriptions.

0.3.0's `broker.connect_backoff` is refused with its replacements named, as
every removed key is ("Loading is strict in both directions").

## Absent port at startup

`[Decided]` The agent keeps running, reports the port as absent on the device
status topic, and counts the failed opens. Source: maintainer, 2026-09-29.

v1 already keeps running and retries with backoff, because a device unplugged at
startup is an expected condition (internal/device/serial/serial.go:84). A
process that exited instead could not publish the status or metrics meant to
report the problem, and a service manager would restart it in a loop until the
device appeared.

Each device's wait before it is opened again is `reopen_interval`, 100 ms by
default, and `reopen_backoff`, shaped as the broker's `reconnect_backoff`
("Reconnecting to the broker"), makes it grow. Unlike the broker's, the reopen
backoff is on by default, from 100 ms doubling to 30 s with 0.3 jitter, v1's
values: every failed attempt is a `port_open_failed` event (#11 Q8), and a fixed
100 ms would publish ten a second for a device left unplugged. They are keys,
not constants, because the maintainer asked that tuning values be settable
(#30).

## Deferred beyond #4

`[Deferred]` Exchange sessions: waiting for a response with a time limit,
refusing concurrent tx, relating responses to requests. Source: maintainer,
2026-09-29.

`[Deferred]` Polling, meaning a timed write followed by a read, which most
scales need. It belongs to the exchange session. Source: maintainer, 2026-09-29.

`[Deferred]` Locking a port and sharing it between processes the way an
operating system does. Source: maintainer, 2026-09-29.

`[Deferred]` Re-reading the configuration on SIGHUP: #6. Source: maintainer,
2026-09-29.

`[Deferred]` Asking the agent where a tx stands, without the question being a
tx: #28. Until then a resend with the same id is the enquiry. Source:
maintainer, 2026-09-29 (#11 Q5).

## Carried over from v1

Still in force. Each decision names the section below that gives its
reasoning. The sections come from v1's DESIGN.md (T12); where v2 changed a fact
or a name, the section says so.

- Configuration loading rejects unknown keys and unknown environment variables,
  and validation reports every problem in one pass ("Loading is strict in both
  directions", "Validation reports every problem, not the first").
- Credentials come from a mode 0600 key=value file or from the environment,
  never from a flag, and a broker URL that carries credentials is rejected
  ("Credentials cannot travel in the broker URL", "The credentials file is
  key=value").
- The instance is the MQTT client id, can be overridden, and defaults to the
  station ("instance_id, not host").
- device_open means the agent holds the port open, not that hardware is
  attached ("device_open, not device_present").
- agent_ts is the host clock in UTC, and nothing in a message vouches for it
  ("The agent does not ask whether the clock is synchronised").
- The connection outlives the run context, the shutdown drain lasts at most
  `delivery.drain_timeout`, and the disconnect is bounded ("The broker
  connection outlives the run context", "The shutdown drain is bounded",
  "Shutdown does not wait on the network without a bound").
- The version is one constant in source. A merge to master with an untagged
  version builds, tags and releases, attaching a tarball and a bare binary per
  platform and one checksums file (README.md, "Continuous integration").
- The container image is Alpine, runs as uid 65532, declares no VOLUME, and
  takes the device as a resolved path given to --device, with --group-add for
  the device's group (README.md, "Container").
- Identifiers name what they hold ("Identifiers say what they hold").

`[Decided]` The release workflow pushes the image to GHCR. Source: maintainer,
2026-09-29.

### Scans are perishable: this agent does not do offline sync

The project principle that offline is the normal case does not apply to this
agent, and that is deliberate. It belongs to handheld terminals, which own a
session and can reconcile later; this agent owns no session. A reading has no
meaning without the session bound to the station at the time, and one buffered
for ten minutes and replayed lands in a pick that has already ended: a corrupt
input, not a late sync (device-agent-spec.md, section 6).

The consequences, in the configuration and checked when it loads:

- Each device's `message_expiry`, 30 s by default, is the MQTT message expiry
  of its readings, so the broker discards a stale reading rather than a
  consumer having to (#23 Q6).
- `delivery.publish_timeout`, 2 s by default, may not exceed any device's
  `message_expiry`: telling an operator that a reading failed after the broker
  had already expired it is worse than useless.
- `delivery.buffer_size`, 64 by default, is a bound, not a target. When it is
  full the reader blocks. A person cannot scan faster than the publisher
  drains, so blocking shows a stalled broker instead of hiding it behind a
  growing queue.
- Nothing pending survives a restart.

The log is the forensic record, not a replay source. A reading the broker did
not take is always recorded with its data; a delivered one carries its data
only when logging.log_payloads asks for it. A delivered reading is upstream, an
undelivered one exists nowhere else, and recording its length alone is how a
station loses data in silence. In v1 this record was the audit log.

Device events are the one thing v2 lets wait for the connection, within their
expiry (#13 Q1). Readings do not wait (#13 Q2).

Do not "fix" any of this by applying the offline-first principle uniformly.

### max_frame_bytes is the payload size, excluding the separator

The specification does not say which. The payload was chosen so that the number
means the same whatever the separator is: switching a device from CR to CRLF
does not change how long a reading may be.

The framer tolerates `len(separator) - 1` bytes past the limit before it calls a
frame over size, so a payload of the maximum length whose separator has only
partly arrived is not rejected one read early
(internal/device/serial/framer.go).

The read buffer is sized from it: one read takes up to `max_frame_bytes` and
the separator, so that a frame which arrives at once is read in one call
(internal/device/serial/serial.go, New). It had fixed bounds of 64 and 4096
bytes until #30.

### Any discard resynchronises to the next separator

Resuming mid-frame after a discard emits the tail of a broken frame as if it
were a short reading. That is silent corruption: a plausible-looking payload no
barcode ever carried. Dropping the remainder is a visible loss the operator can
act on, for the reason in "Scans are perishable": an operator who knows a scan
did not land scans again.

Resynchronisation ends at the next separator, or when the device falls silent
for one inter-character timeout.

### Choosing inter_char_timeout

The timeout ends resynchronisation as well as starting it, and that has a limit
worth stating. If a device stalls mid-frame for longer than one timeout, falls
silent, and then sends the rest of the frame, the tail is taken for a new frame
and can be published as a short payload. The information needed to tell that
tail from a genuine new scan does not exist at this layer.

The mitigation is the timeout's value. At 9600 baud a byte takes about a
millisecond, and a scanner sends a scan as one continuous burst, so the 200 ms
default is roughly 200 byte-times of slack. Set it above any gap that occurs
inside a real burst on the hardware in question, and it will not fire mid-frame.

The alternative, resynchronising until a separator arrives however long that
takes, was rejected because it makes the wrong-separator failure quiet. A
scanner sending LF where the configuration says CR would emit one warning and
then discard every later scan at DEBUG. Ending resynchronisation on silence
produces one warning per scan, the loud failure that the manual acceptance
checklist of device-agent-spec.md, section 11, expects. In v2 each of those
discards is also an event and a count in the keepalive, which is how an
unmatched separator is seen from outside the station ("Reading: rx").

### A CR/CRLF mismatch is the one wrong separator that is not loud

Measured on a Symbol 05e0:1701, not reasoned about. When the device sends CRLF
and the configuration says CR, the frame splits correctly on the CR and an
orphaned `0x0A` is left buffered. Scanning slowly, the inter-character timeout
discards it and nothing is lost. Scanning faster than the timeout, the stray
byte is still buffered when the next scan arrives and is **prepended to it**,
producing a frame that is published with `text_valid: true` and cannot be told
from a genuine barcode. The same capture also lost a scan to a resync discard
caused by the previous stray byte.

So this mismatch corrupts under load and looks fine at a desk, which is the
failure device-agent-spec.md, section 4.5, describes, and the reason a
per-model configuration sheet is not optional. The full capture is in
docs/scanners/symbol-05e0-1701.md.

A cheap defence exists and is **not implemented**: a frame whose first byte
belongs to a common separator that is not part of the configured one is almost
certainly this mismatch. v1 left open whether such a frame should be dropped
or published with a warning, as an operational decision rather than a
technical one. v2 settles it: stray bytes are fixed in the configuration or by
IT, and the agent does not compensate for them ("Reading: rx"), so the defence
is not built.

### The backoff resets on session duration, not on a successful open

device-agent-spec.md, section 4.4, says not to spin. Resetting the reopen
backoff whenever the port opened and read at least once does spin: a failing
cable lets the port enumerate, open and return one read timeout before it
drops, so the backoff returns to its initial value on every cycle. Measured in
v1 against an injected failing port, that produced 87 reopens a second,
indefinitely.

The backoff resets only when a session lasted at least the backoff's ceiling,
`reopen_backoff.max`, 30 s by default (internal/device/serial/serial.go, Run).
A device that has been up for half a minute counts as healthy and reconnects
promptly; one that flaps backs off to the ceiling. The same measurement
produced 7 reopens per second and climbing.

The threshold is the ceiling rather than a value of its own, so that the
reopen has one set of values (#30).

### The agent does not ask whether the clock is synchronised

`agent_ts` is the host's clock in UTC, and nothing in a message vouches for it.
An early v1 version queried `adjtimex` on Linux and published a `clock_synced`
flag; that was removed, with its package.

Knowing whether a clock is disciplined is not this agent's job. It is a
transport (device-agent-spec.md, section 1), the ingest side has its own clock
to compare against, and a flag that could only be answered on Linux invited
consumers to trust a timestamp because one platform said so. Hosts without an
RTC still boot with a fictional wall clock; that is a fleet provisioning
problem.

### The whole frame is the payload

`raw_b64` is the complete frame with only the separator removed. Nothing is
stripped.

The alternative was to strip a code identifier and report it. Real hardware
ruled that out. A Symbol 05e0:1701 prefixes every scan with a Symbol Code
Character whose length is not constant: one byte for the 1D symbologies, three
for the 2D ones, told apart only by whether the first byte is `P`. A fixed
prefix length cannot express that, and a substring search is worse than
useless: `DAP00838418592059` is a Code 128 whose data begins `AP00`, so matching
on `P00` anywhere would mangle it.

Delimiting the identifier therefore needs a per-vendor table, and that table is
domain knowledge a transport must not carry ("Scope"). Upstream already holds
per-device definitions and can decode the identifier along with everything
else, from a payload that has not been altered on the way. The table for the
scanner in hand is in docs/scanners/symbol-05e0-1701.md, for the upstream side.

v1 kept a `symbology` field, always null, because the specification's payload
had one; v2 drops it ("rx").

### The broker connection outlives the run context

device-agent-spec.md, section 10, shuts down on SIGTERM by draining and
exiting, and the agent ends that sequence with its offline message and a clean
DISCONNECT. Dialling the connection with the context that the signal cancels
breaks both: measured in v1 against the development broker, the offline status
failed with "no connection available" and the broker delivered the will
instead, reporting a crash where there had been an orderly stop.

The connection therefore has its own context, cancelled only after the core
returns (cmd/skuhus-device-agent/run.go, runAgent). The core's shutdown order
is: the keepalive stops, the devices stop, the buffers drain, the offline
message goes out, then DISCONNECT (internal/core/core.go, Run). The end-to-end
test checks it (T10).

### The shutdown drain is bounded

A frame already in the buffer gets its full `publish_timeout`, but the drain as
a whole stops after `delivery.drain_timeout`, 5 s by default
(internal/core/core.go, Run). Without a bound, a full buffer against an unresponsive broker holds the process open for
`buffer_size` times `publish_timeout`, which at the defaults is over two minutes
spent delivering readings whose sessions have ended ("Scans are perishable").
What is left is recorded in the log as dropped, with its data, not discarded
silently. Device events still waiting for the connection end at the same
deadline.

The bound was an internal constant, 5 s, until the maintainer asked that tuning
values be settable (#30).

A tx being written when the agent starts to stop has the same bound before the
readers stop, since its port has to stay open (#19 Q1). It stops after the
chunk in hand, and that chunk cannot be cut short: go.bug.st/serial writes on a
blocking descriptor. On the bench adapter, which takes 16 KB at a time, the
chunk in hand returned 9.8 s after the drain's 5 s, and closing the port took
another 4.5 s, most likely while the driver sent the block it held; the whole
stop took 19.3 s. A write deadline needs a port layer of the agent's own, the one #5
proposes.

### Shutdown does not wait on the network without a bound

Disconnecting writes a DISCONNECT packet, which means writing to a socket that
may be attached to a network that has gone away. The disconnect is therefore
given `publish_timeout` rather than an unbounded context: by then the agent has
published everything it had, and an unbounded wait turns "the WAN dropped" into
"the service will not stop".

Measured in v1 against a frozen broker, its container paused so that the socket
stays established and nothing is refused: a SIGTERM took two seconds, the
offline status timing out at its own bound and the disconnect returning.

### A failed publish is not retried

Retrying is what the delivery semantics of device-agent-spec.md, section 6,
rule out. By the time a retry lands the reading is stale, and the operator who
sees no confirmation scans again. The failure is logged with the reading's data
and counted: the keepalive's `publish_failures` shows a station that is reading
but not delivering, without anyone reading its log.

autopaho's publish queue is left nil for the same reason
(internal/transport/mqtt/client.go). With a queue, a publish made while
disconnected is accepted and sent on reconnection, which is precisely the
offline replay this agent must not do.

### device_open, not device_present

The flag says one thing only: this agent holds the device's port open. It is
true once the port opens, and false when an open fails, the port is lost, or
the agent closes it (internal/core/core.go, count).

"Present" reads as a statement about the hardware, and would be wrong in the
case that matters most. A scanner that is plugged in and enumerated but held by
another process, or refused by permissions, is present and unusable; reporting
it as present hides exactly the failure an operator is looking for. What the
agent knows is whether it has the port, so that is what the field says.

### instance_id, not host

device-agent-spec.md, section 5.4, carries `host`, and a hostname is the wrong
identifier: it is not unique across a fleet, it changes under DHCP, and it
cannot tell apart two agents on one machine.

`instance_id` is `identity.instance`, defaulting to `identity.station`, and it
is the MQTT client id. It can be set because a client id must be unique per
broker connection: two processes sharing one disconnect each other in a loop.
So the same value names the agent in every message and names its connection on
the broker, and `rabbitmqctl list_mqtt_connections` maps straight onto the
messages.

The machine name is not lost. It is an attribute of every log record, where
"which box is this" is the question being asked, and it is kept out of the
messages, where it never identified anything.

v1's topics stopped at the station, so two instances at one station shared
their status topic. Measured: a second instance shutting down published a
retained `offline` for a station whose first instance was still running and
scanning. v2 puts the instance into the agent's topic,
`agent/<instance>/status` ("Topics").

### Credentials cannot travel in the broker URL

`broker.url` carrying user information is a validation error, not a supported
way to authenticate. A URL is visible in the process list, in every log line
that names the broker, and in a configuration file pasted into a ticket, which
is the whole reason device-agent-spec.md, section 8, puts credentials in a mode
0600 file. The error does not quote the URL back, because that would put the
password into the output of whoever ran `validate`.

As a second line, `Broker.RedactedURL` is what the log and the `validate`
summary print (internal/config/config.go).

### The credentials file is key=value

The specification names `broker.credentials_file` but not its format. It is
`username=` and `password=` lines, with `#` comments, rather than two bare
lines, so that a file edited by hand cannot silently swap the two. The password
is taken verbatim after the first `=`: a password may legitimately end in a
space, and trimming one produces an authentication failure that reads as a
broker fault (internal/config/credentials.go). `SH_DEV_AGENT_MQTT_USERNAME` and
`SH_DEV_AGENT_MQTT_PASSWORD` override the file.

They name MQTT rather than the broker because that is what they authenticate.
The configuration section stays `broker:`, so the environment mixes both names:
`SH_DEV_AGENT_BROKER_URL` and four other variables name the broker.

### Loading is strict in both directions

An unknown key in the configuration file and an unrecognised `SH_DEV_AGENT_*`
variable are both fatal. A misspelled setting that is silently ignored leaves a
station running a value the operator believes they changed, and the fleet then
disagrees with its own configuration management. A key or variable that an
earlier release read and 2.0.0 does not is refused with its replacement named
(T5).

Devices are not settable from the environment. A list does not map onto flat
variables without inventing an indexing scheme.

### Validation reports every problem, not the first

A misconfigured station is fixed in one pass rather than one restart per typo.

### Checks that exist because of a specific failure

- A `separator` containing a backslash is rejected, with the YAML quoting
  explained. `separator: '\r'` in single quotes is the two characters backslash
  and r, and produces a device that frames nothing and logs a timeout per scan.
- `/dev/tty.*` is rejected wherever it appears, not only on macOS, and the error
  names the `/dev/cu.*` twin. A configuration written for a Mac fails
  validation on the CI machine too.
- `/dev/ttyACM<n>` and its kind warn rather than fail. They work; they move
  between reboots and with replug order.
- A `credentials_file` readable by group or other is rejected. Credentials
  everyone on the host can read are not per-station credentials.
- `delivery.publish_timeout` longer than any device's `message_expiry` is
  rejected ("Scans are perishable").

### One name for one thing

v1 was named skuhus-device-serial-scanner rather than skuhus-agent, the name
the specification uses in sections 9, 10 and 13.2. Its binary was called
skuhus-agent until a release carried two names for one thing:
`skuhus-agent-0.1.0-linux-arm64.tar.gz` beside an image at
`ghcr.io/skuhus/device-serial-scanner`. And skuhus-agent names a category: on a
station that also ran a printer agent, `/etc/skuhus-agent/`, the
`SKUHUS_AGENT_*` environment, the log directory, the system user, the systemd
unit and the process in `ps` would all have collided.

So one name is used for the binary, the configuration directory, the log
directory, the release assets, the container image and the account inside it.
v2's name is skuhus-device-agent, with the environment prefix SH_DEV_AGENT_
("Naming").

### Identifiers say what they hold

Receivers are `agent`, `client`, `framer`, `presence` rather than `a`, `c`,
`f`, `p`, and locals are named for their contents rather than for their type's
first letter. This costs a few characters a line and pays for itself the first
time someone reads a function they did not write. The exceptions are `err`,
`ok`, `ctx` and `t *testing.T`, which are read as punctuation rather than as
names.

Renaming has one hazard worth recording, because it happened in v1: a
mechanical rename collided with an existing variable in `Validate`, turning
`problems, warning := validateBroker(...)` followed by
`append(problems, problems...)` into a function that discarded every problem
found so far. It compiled. The tests caught it, which is the argument for tests
that assert on rejections and not only on acceptances.

## Logging: one common log

`[Decided]` There is one log, and everything is written to it. The separate
audit log is removed, and delivery outcomes become records in the common log.
Source: maintainer, 2026-09-29 ("Let's just call it a common log, and write all
logs in there").

`[Decided]` Every record carries its severity and identifies where it was
produced. Source: maintainer, 2026-09-29.

`[Decided]` The identification is slog's source attribute on every record,
which gives the package-qualified function name, the file and the line
(log/slog, type Source). v1 has the source option and leaves it off
(internal/logging/logging.go:32-33). The proposed hand-written component
attribute is dropped. Source: maintainer, 2026-09-29 (#23 Q10).

`[Deferred]` A standard for errors, with identifiers. Source: maintainer, 2026-09-29 (#23 Q10).

`[Carried over]` Every rx and tx message gets a record of what happened to it,
and the record carries the payload when the broker did not accept the message
("Scans are perishable"). In v1 this was the audit log.

A reading's record is its line in the log: `rx published` at INFO, `rx publish
failed` at ERROR, `rx dropped` at WARN, or, if it could not be encoded, `rx
could not be encoded` at ERROR. Each carries the message's `id`, `device_id`,
`seq`, `bytes` and `text_valid`, and `outcome` set to `published`, `failed` or
`dropped`. It is written whatever logging.level says, as the audit file was.
Data read from a port appears on a line as `data_hex`, and as `data_text` as
well when it is valid UTF-8. A failed or dropped reading always carries its
data; a published one and a discard warning carry it only with log_payloads.

The configuration's warnings, and the error that stops the agent, are in the
log as well, once it exists. A failure to rotate the log file is reported on
stderr, and writing goes on in the live file past its limit until the cause is
cleared. A record that cannot be written is reported on stderr too, and the
agent goes on reading: a station that cannot write its log is degraded, and one
that stops because a disk is full is out of service, which is worse for the
people using it.

The connection library logs its own lines through the same log. Once the agent
has closed the connection, the library's lines are discarded: it cannot tell
when it has finished shutting down (autopaho/auto.go, Done), and its goroutines
went on logging, a few milliseconds after the log file had closed.

`[Decided]` The log can go to a file with size rotation, configured by
logging.file, logging.max_size_mb and logging.keep in place of audit_file,
audit_max_size_mb and audit_keep, and to stdout, so that journald and `docker
logs` show it. Each destination is optional, so a deployment picks the
combination that suits it. Source: maintainer, 2026-09-29 (#23 Q11).

`[Decided]` A configuration with neither destination is accepted: where the agent
logs, if anywhere, is the operator's business. Source: maintainer, 2026-09-29 (#23 Q11a).

`[Decided]` Records at INFO and above are flushed to disk as they are written,
as v1's audit log was, and for its reason: a station loses power without
warning, and a record that ends several readings before the lights went out
cannot answer the question it exists for. They include every delivery
outcome. DEBUG records are not flushed individually, because at DEBUG every
read from the port is a record. Chosen at the maintainer's request (#23 Q12).

Flushing makes the kernel put a record on the disk before the agent continues,
so it survives a power cut. At the rate a person scans, that is a few writes per
scan, which a station's storage absorbs; at the rate a streaming device produces
INFO records it would not be, and that is the point to revisit if one is
connected. Flushing concerns the log file only: records on stdout are the
receiving side's to keep.

`[Decided]` With log_payloads set, the data read from the port appears on the
INFO line that reports the frame. In v1 it appears only at DEBUG
(internal/device/serial/serial.go:281), so seeing a reading means switching on
every other DEBUG line too. The maintainer reported this as a defect on
2026-09-07. Source: maintainer, 2026-09-29 (#23 Q13).

## Broker constraints

Measured in M0 (docs/spikes/m0-mqtt5.md):

- The agent needs MQTT 5. The broker at 10.9.21.23 is RabbitMQ 3.10.25 and does
  not accept MQTT 5, so the agent cannot connect to it until it is upgraded or
  replaced.
- RabbitMQ 4.1.8 and 4.3.5 support message expiry, retained messages, response
  topic and correlation data. The maximum QoS is 1.
- A will is delivered but not retained, and retained messages are not replicated
  across cluster nodes.
- A persistent session queues QoS 1 messages for a disconnected client and
  honours their message expiry.
- On 4.3.5 a retained message reaches only a subscription naming its exact
  topic, not a wildcard subscription. Measured on 2026-09-29, during T4.
- On 4.3.5 a QoS 1 publish can be answered with PUBACK reason 0x83 although it
  was delivered. Seen on 2026-10-04, during T14: two subscribers waited for a
  tx result each, and the second result was published just as the first
  subscriber disconnected. The second subscriber received it, and the agent got
  0x83 and recorded the publish as failed. The agent cannot tell this from a
  refusal, so a reading published at such a moment would be recorded as failed
  too.

## Implementation approach

The maintainer's assessment of v1, given before the #4 discussion: it does what
is needed, and it is in a state where starting over is easier than fixing it.

`[Decided]` Build v2 as a new core. Source: maintainer, 2026-09-29 (#23 Q14). Whatever turns out to be
needed can be carried over; what was measured or tested against a real failure
comes across first:

- the framer and its tests, including the real capture in
  internal/device/serial/testdata;
- the configuration validation rules, each of which exists because of a specific
  failure ("Checks that exist because of a specific failure");
- the transport's connection handling: bounded disconnect, connection lifetime
  independent of the run context, no publish queue;
- the measurements in docs/.

Leave behind:

- the supervisor in internal/agent, which owns publishing, status, heartbeat,
  presence and the drain policy in one type;
- runAgent (cmd/skuhus-device-serial-scanner/run.go:90-243), 154 lines that
  assemble logging, audit, credentials, devices, transport and supervisor in one
  function;
- validation duplicated between internal/config and agent.New;
- dev/ and spike/ inside the agent's module, where `go vet ./...` and `go test
  ./...` compile them, so a tool that stops compiling fails the agent's checks
  (T4).

## Open decisions

None. The maintainer's answers to #23 and, for the message formats, to #11,
both on 2026-09-29, settled every proposal and follow-up, and later questions
were answered on their tickets; they are recorded in their sections above.

Task numbers refer to PLAN-V2.md.
