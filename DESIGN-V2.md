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

`[Proposed]` A device id is now a topic segment, so it follows the topic-segment
rule `[a-z0-9-]+` (internal/config/validate.go:24). v1 allows `[A-Za-z0-9._-]+`
(validate.go:27), which was acceptable while the id only appeared inside
payloads.

## Message ids and execution results

`[Decided]` Every rx and every tx message carries a UUID. Source: maintainer,
2026-09-29.

- rx: generated by the agent, one per frame, as v1's event_id.
- tx: supplied by the sender.

`[Decided]` For each tx the agent publishes an execution result naming the tx id,
so the sender learns what happened to it. Source: maintainer, 2026-09-29.

`[Proposed]` The result goes on the device's status topic, with one of three
states: accepted (received and queued for the port), written (every byte reached
the port), failed (with a reason and, for port errors, the error class v1
already assigns: absent, busy, permission_denied, read_only, disconnected).

`[Proposed]` The tx id is an idempotency key. The agent remembers recent tx ids
and does not write a tx whose id it has already written; it publishes a result
saying so instead. A sender that saw no result can then resend safely.

`[Proposed]` The tx carries a sender field, set by the sender. Several senders
may write to one device and there is no session, so the sender field is the
only record of who wrote what when an interleaving has to be reconstructed.

`[Proposed]` The id travels in the JSON payload, not as MQTT 5 correlation data.
Correlation data works on RabbitMQ 4.x (docs/spikes/m0-mqtt5.md:100), but a
payload field is visible in every log and to every consumer, and does not depend
on the broker supporting the property.

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

`[Decided]` The failure that matters most is a separator configured the wrong way
round, so that no reading is ever emitted. Source: maintainer, 2026-09-29. The
Symbol 05e0:1701 capture shows what it looks like: one inter_char_timeout
discard per scan and no frames (docs/scanners/symbol-05e0-1701.md:32).

`[Proposed]` That failure is detected upstream from the counters, not in the
agent: for one device, rx bytes rising, rx frames flat, timeout discards rising.

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

`[Proposed]` The message expiry becomes a per-device setting. v1 has one
delivery.scan_ttl for the whole process (internal/config/config.go:114); a
scanner and a scale on one station need not give their readings the same
lifetime.

`[Proposed]` A device may declare a model string that is published in every rx
message, so a consumer can choose a parser from the message rather than from a
registry mapping stations to hardware.

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

`[Proposed]` A tx is written to the port contiguously. Senders are not ordered
against each other, but the bytes of two tx messages are never interleaved on
the wire: one writer per port.

`[Proposed]` A tx for a port that is not open fails at once, with the port's
error class in the result.

`[Proposed]` A tx published while the agent is reconnecting is not lost silently.
v1 connects with clean start and session expiry 0
(internal/transport/mqtt/client.go:107-108), so no session survives a
disconnect. While the agent reconnects its tx subscription does not exist; a tx
published in that window has no subscriber, the broker drops it, and no result
is ever published. Two measures, used together:

- The contract states that a sender that receives no result within a stated time
  treats the tx as not written.
- The tx subscription uses a persistent session, and senders set a message
  expiry on every tx. The broker queues a tx through a short disconnect and
  drops it once stale. M0 measured both on RabbitMQ: a 2 s message expired
  during a 6 s absence while a 300 s one was delivered
  (docs/spikes/m0-mqtt5.md:108).

This reverses v1's clean start for the tx direction only. v1 chose clean start
because a queued stale command is harmful; the sender's message expiry is what
makes queuing safe here.

`[Decided]` The serial library stays go.bug.st/serial. Source: maintainer,
2026-09-29. It supports data bits, parity and stop bits (its `Mode`), keeps the
error code when a write fails, and reports a read timeout as zero bytes rather
than end of file; v1's read path is tested against it with a real scanner.

`[Deferred]` Flow control and pacing: #5. Source: maintainer, 2026-09-29.
go.bug.st/serial always switches flow control off (serial_unix.go:242 and
422-423 in v1.8.0), and no alternative library does better; #5 records the
evaluation and the proposed replacement on `*os.File` and golang.org/x/sys/unix.
Printers on `/dev/usb/lpN` or raw TCP use no termios and are not affected.

`[Proposed]` The write path protects itself from two properties of the library.
Its `Write` takes no lock and does not check that the port is open, so a write
racing a close reaches whatever descriptor now has that number; and it makes one
write(2) call without continuing after a partial write (serial_unix.go:112-118).
The agent's port type holds one lock across `Write` and `Close`, and loops until
every byte is written.

## Status channel

`[Decided]` The agent publishes keepalives on the status channel, so that every
agent and every device can be seen from outside. Source: maintainer, 2026-09-29.

`[Decided]` Device problems are reported through the status channel as well as
the log, "not only via logs on the host computer". Source: maintainer,
2026-09-19.

`[Proposed]` Device events on the device status topic: port opened, port closed,
port lost with its error class, discard with its reason and byte count, tx
result.

`[Proposed]` Counters in the agent keepalive, per device: rx frames, rx bytes,
discards by reason, failed opens by error class, publish failures, tx written,
tx failed, buffer depth. The v1 heartbeat carries some of these
(internal/event/heartbeat.go:24-38). They are pushed over MQTT rather than
scraped over HTTP: stations are not generally reachable for scraping, and
publishing keeps the numbers off the host, which is the aim of the design.

`[Carried over]` Liveness comes from the keepalive, not from the retained
status. RabbitMQ delivers a will without retaining it, and does not replicate
retained messages across cluster nodes (docs/spikes/m0-mqtt5.md:102-106). A
consumer relying on the retained status sees a dead agent as online.

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

`[Proposed]` The identification is slog's source attribute on every record,
which gives the package-qualified function name, the file and the line
(log/slog, type Source), plus a component attribute: port, framer, transport,
publisher or status. The component survives refactoring, where a function name
changes whenever code moves. v1 has the source option and leaves it off
(internal/logging/logging.go:32-33).

`[Carried over]` Every rx and tx message gets a record of what happened to it,
and the record carries the payload when the broker did not accept the message
(DESIGN.md, "Scans are perishable"). In v1 this was the audit log.

`[Proposed]` The log is written to a file with size rotation, configured by
logging.file, logging.max_size_mb and logging.keep in place of audit_file,
audit_max_size_mb and audit_keep; the same records also go to stdout, so that
journald and `docker logs` show them.

`[Proposed]` Records at INFO and above are flushed to disk as they are written,
as the audit log was (DESIGN.md, "The audit log is flushed per record"). They
include every delivery outcome. DEBUG records are not flushed individually,
because at DEBUG every read from the port is a record.

`[Proposed]` With log_payloads set, the data read from the port appears on the
INFO line that reports the frame. In v1 it appears only at DEBUG
(internal/device/serial/serial.go:281), so seeing a reading means switching on
every other DEBUG line too. The maintainer reported this as a defect on
2026-09-07.

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

## Implementation approach

The maintainer's assessment of v1, given before the #4 discussion: it does what
is needed, and it is in a state where starting over is easier than fixing it.

`[Proposed]` Build v2 as a new core, and bring across only what was measured or
tested against a real failure:

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
- dev/ and spike/ inside the agent's module, where the M0 spike put paho.golang
  into go.mod before any agent code used it.

## Open decisions

No item is open. 19 items are still `[Proposed]`, 14 of them blocking phase 1
tasks; #23 lists each with the tasks it blocks, and settles them. Until an item
is marked `[Decided]`, the legend applies: do not build on it.
Decisions of 2026-09-29 are recorded in their sections above.

Task numbers refer to PLAN-V2.md.
