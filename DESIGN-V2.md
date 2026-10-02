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
- `[Carried over]` - a v1 decision that still holds. Its reasoning is in
  DESIGN.md under the heading named.

"Maintainer" means Pavel Kim in the #4 design discussion, with the date. File
references are to commit f97c736, before the v2 rename moves the files.
DESIGN.md is the v1 design; this document supersedes it where they disagree.

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

The v1 read path is already device-neutral. The framer knows a terminator, a
maximum frame size and an inter-character timeout
(internal/device/serial/framer.go). The only scanner-specific text in the
device layer is the assert_config warning (internal/device/serial/serial.go:212).
The v1 envelope already carries the whole frame unmodified (DESIGN.md, "The
whole frame is the payload"). The conversion is therefore mostly removal and a
new topic and message layout, not new reading logic.

## Version: 2.0.0, and 1.0.0 is never used

`[Decided]` The first release of the device agent is 2.0.0. 1.0.0 was the target
of the scanner agent, which was never released under it; the number is skipped
and not reused. Source: maintainer, 2026-09-29.

Builds before that stay on 0.x; the constant is 0.3.0
(internal/version/version.go). The release workflow releases whenever a merge
to master carries a version that has no tag yet (.github/workflows/release.yml),
so 2.0.0 is set in the pull request that completes the release scope.

`[Decided]` 2.0.0 releases reading; 2.1.0 releases writing. Source: maintainer,
2026-09-29. With the topic layout reserving tx and the status channel in place
from 2.0.0, writing adds topics and messages without changing any existing one,
which is a minor version under semantic versioning.

## Naming

`[Decided]` The repository is skuhus/device-agent, renamed on 2026-09-29. The Go
module path follows it: github.com/skuhus/device-agent, changed by T3 (#8).

`[Carried over]` One name for the binary, the configuration directory, the log
directory, the release assets, the container image and the account inside it
(DESIGN.md, "The binary is skuhus-device-serial-scanner, not skuhus-agent").

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

`[Decided]` Status at two levels. Source: maintainer, 2026-09-29.

    skuhus/<project>/<site>/<station>/agent/<instance>/status   will, keepalive, counters
    skuhus/<project>/<site>/<station>/<device>/status           device events, tx results

- A will is registered on one topic per connection
  (internal/transport/mqtt/client.go:143), so "this agent is gone" cannot be
  published on each device's topic.
- The instance is in the path because a station-level status collided when two
  agents shared a station: one instance shutting down published a retained
  offline for a station whose other instance was running and scanning
  (DESIGN.md, "instance_id, not host").
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

`[Carried over]` Framing rules, each with the measurement behind it in DESIGN.md:

- max_frame_bytes counts the payload and excludes the terminator
  ("max_frame_bytes is the payload size, excluding the terminator").
- After any discard the framer drops bytes up to the next terminator ("Any
  discard resynchronises to the next terminator").
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
stray byte is discarded or prepended to the next reading (DESIGN.md, "A CR/CRLF
mismatch is the one wrong terminator that is not loud").

`[Decided]` That failure is detected upstream from the counters, not in the
agent: for one device, rx bytes rising, rx frames flat, timeout discards rising.
Source: maintainer, 2026-09-29 (#23 Q5).

`[Deferred]` Framing for devices that send no separator, ended by a timeout, a
size, or both: #7. Source: maintainer, 2026-09-29, as no device in use needs it.
v1 discards on both triggers because publishing the tail of a frame produced
plausible, wrong readings when a CRLF device was configured as CR (DESIGN.md, "A
CR/CRLF mismatch is the one wrong terminator that is not loud"). #7 records the
constraints: chosen per device, never the default where a separator is
configured, and the size semantics settled with a device on the bench.

`[Carried over]` rx is published at QoS 1 with a message expiry, through a
bounded buffer, and is never retried or replayed (DESIGN.md, "Scans are
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
(internal/device/serial/serial.go, classify). `error` is the operating system's
message. `reason` is one of `oversize`, `inter_char_timeout`, `resync` and
`empty_frame`, the framer's names (internal/device/serial/framer.go), and
`bytes` is how many bytes were discarded.

`[Decided]` `port_open_failed` is published on every attempt to open the port,
which keeps the rule simple. Source: maintainer, 2026-09-29 (#11 Q8). A scale
unplugged overnight, retried every 30 s at most (v1's backoff limit,
internal/device/serial/serial.go:27), publishes about 1,440 of them in 12
hours.

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
at once and is recorded in the log file with its payload, as before (DESIGN.md,
"Scans are perishable"): a reading that arrives late can make a consumer act on
a scan the operator has already repeated. Source: maintainer, 2026-10-02 (#13
Q2).

The port opens before the broker connection on every start, so `port_opened` is
one of the events that waits (measured in #13).

### tx

On `<device>/tx`, published by senders. Reserved in 2.0.0: the agent subscribes
to it from 2.1.0.

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
written gets `rejected` alone, and the earlier one carries on.

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
| `written` | `already_written` | `written_at` | A tx with this id was written before, so this one was not (#23 Q15). The agent remembers a bounded number of written ids in memory, and a restart forgets them (#11 Q6a, T16). |
| `rejected` | `in_progress` | `stage`, `since`, `bytes_written` | A tx with this id is queued or being written, so this one is not taken (#11 Q6). |
| `failed` | `invalid_message` | `error` | Not JSON, `schema` is not 2, a field is missing or unknown, or `raw_b64` is not base64. |
| `failed` | `invalid_id` | | `id` is not a UUID. |
| `failed` | `port_unavailable` | `error_class`, `error`, `open_attempts` | The port could not be opened in the configured number of attempts (#23 Q17). |
| `failed` | `write_failed` | `error_class`, `error`, `bytes_written`, `open_attempts` | Writing started and failed. It is not retried, and `bytes_written` says how far it got (#23 Q17a). |
| `failed` | `expired` | `open_attempts` | The tx's message expiry passed before an attempt could start (#23 Q17a). |

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
device event does, and the device's counters since the process started, the
list in "Status channel" (#23 Q9):

| Counter | Counts |
|---|---|
| rx_frames | Frames taken for publishing, whatever became of them: the last rx `seq`. |
| rx_bytes | Every byte read from the port, separators and discarded bytes included, so that it rises while `rx_frames` stays flat when nothing is framed (#23 Q5). |
| discards | Discards by reason, one per `bytes_discarded` event. |
| failed_opens | Failed attempts to open the port by error class, one per `port_open_failed` event. |
| publish_failures | Readings the broker did not take: failed, or dropped at the shutdown drain. Each has a record in the log file. |
| tx_written, tx_failed | Tx results, from 2.1.0; 0 until then. |
| buffer_depth | Frames waiting to be published now. |

Every reason and every class is present, at 0 when nothing happened, so a
consumer can difference two keepalives without handling a missing key. An
unmatched separator ("Reading: rx") shows as `discards.inter_char_timeout` and
`rx_bytes` rising while `rx_frames` stays flat.

Besides every interval, a keepalive goes out as soon as the broker connection
comes up. The first one does not wait an interval, and a consumer that saw a
will learns within a moment that the agent is back.

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

## Absent port at startup

`[Decided]` The agent keeps running, reports the port as absent on the device
status topic, and counts the failed opens. Source: maintainer, 2026-09-29.

v1 already keeps running and retries with backoff, because a device unplugged at
startup is an expected condition (internal/device/serial/serial.go:84). A
process that exited instead could not publish the status or metrics meant to
report the problem, and a service manager would restart it in a loop until the
device appeared.

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

Still in force. The reasoning is in DESIGN.md under the heading named.

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
- The connection outlives the run context, the shutdown drain lasts at most 5 s,
  and the disconnect is bounded ("The broker connection outlives the run
  context", "The shutdown drain is bounded at 5 seconds", "Shutdown does not
  wait on the network without a bound").
- The version is one constant in source. A merge to master with an untagged
  version builds, tags and releases, attaching a tarball and a bare binary per
  platform and one checksums file (README.md, "Continuous integration").
- The container image is Alpine, runs as uid 65532, declares no VOLUME, and
  takes the device as a resolved path given to --device, with --group-add for
  the device's group (README.md, "Container").
- Identifiers name what they hold ("Naming").

`[Decided]` The release workflow pushes the image to GHCR. Source: maintainer,
2026-09-29.

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
(DESIGN.md, "Scans are perishable"). In v1 this was the audit log.

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
cleared.

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
as the audit log was (DESIGN.md, "The audit log is flushed per record"). They
include every delivery outcome. DEBUG records are not flushed individually,
because at DEBUG every read from the port is a record. Chosen at the
maintainer's request (#23 Q12).

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

## Implementation approach

The maintainer's assessment of v1, given before the #4 discussion: it does what
is needed, and it is in a state where starting over is easier than fixing it.

`[Decided]` Build v2 as a new core. Source: maintainer, 2026-09-29 (#23 Q14). Whatever turns out to be
needed can be carried over; what was measured or tested against a real failure
comes across first:

- the framer and its tests, including the real capture in
  internal/device/serial/testdata;
- the configuration validation rules, each of which exists because of a specific
  failure (DESIGN.md, "Checks that exist because of a specific failure");
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

None. No item is open or proposed. The maintainer's answers to #23 and, for the
message formats, to #11, both on 2026-09-29, settled every proposal and
follow-up; they are recorded in their sections above.

Task numbers refer to PLAN-V2.md.
