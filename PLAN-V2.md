# Plan v2: subtasks of #4

The work that turns the scanner agent into the device agent described in
DESIGN-V2.md. Each task is sized for one pull request, except where it says
otherwise.

Task numbers are referenced from DESIGN-V2.md, "Open decisions". Decision
markers ([Decided], [Proposed], [Open]) have the meanings given there. File
references are to commit f97c736.

Phase 1 delivers reading and is released as 2.0.0. Phase 2 delivers writing and
is released as 2.1.0. Decided by the maintainer on 2026-09-29.

Postponed out of #4 into their own tickets: #5 flow control and write pacing
(formerly T17), #6 reloading the configuration on SIGHUP, #7 framing for devices
that send no separator.

## Tracking

Every task is a sub-issue of #4, in this order. T1 was finished before issues
were created for the plan, so it has none. #6 and #7 are sub-issues of #4 too,
postponed beyond it, as are #20 (formerly T15), #26 and #27. #24 and #25 are
defects found in review, fixed within T11 and T4.

| Task | Issue | Title |
|---|---|---|
| T1 | none, done | Commit the measurements in docs/ |
| T2 | #23 | Settle the open decisions |
| T3 | #8 | Rename to the device agent |
| T4 | #9 | Give spike/ and dev/ their own modules |
| T5 | #10 | Configuration schema v2 |
| T6 | #11 | Topics and message formats |
| T7 | #12 | Core: reading and publishing |
| T8 | #13 | Status channel and counters |
| T9 | #14 | Common log |
| T10 | #15 | End-to-end test in CI |
| T11 | #16 | Development environment for v2 |
| T12 | #17 | Documentation |
| T13 | #18 | Release 2.0.0 |
| T14 | #19 | tx: receive and write |
| T15 | #20 | tx through reconnects, postponed out of #4 |
| T16 | #21 | tx idempotency |
| T17 | #5 | Flow control and write pacing, postponed out of #4 |
| T18 | #22 | Release 2.1.0 |

## Order

    T1 commit docs ----+
    T2 decisions ------+--> T3 rename --> T4 tool modules
                       |        |
                       |        +--> T5 config --+
                       |        +--> T6 messages +--> T7 core --> T8 status --+
                       |                         |           +--> T9 logging  |
                       |                         +--> T11 dev env ------------+--> T10 e2e --> T12 docs --> T13 release 2.0.0
                       |
                       +--> T14 tx --> T16 tx idempotency --> T18 release 2.1.0

T3 goes first among the code tasks so that every later pull request is written
against the final names, and the rename is a single diff with no behaviour
change in it.

## Phase 0: before code

### T1. Commit the measurements in docs/

Status: done on branch gh-4-docs, committed at the maintainer's request on
2026-09-29, together with dev/, spike/ and these two documents.

Motivation. DESIGN-V2.md cites docs/spikes/m0-mqtt5.md for every broker
constraint and docs/scanners/symbol-05e0-1701.md for the signature of an
inverted separator. Neither file is tracked: at f97c736, `git status` shows
`?? docs/`, `?? dev/` and `?? spike/`. A clone of the repository does not
contain them, so those citations point at nothing. The measurements took a
scanner on the desk and access to the fleet broker to produce, and cannot be
regenerated from the code.

Work. Decide what in dev/, docs/ and spike/ belongs in the repository, and
commit it. docs/ at least. dev/ holds the local broker configuration and the
consumer that T11 updates. spike/ holds the M0 tools, which T4 moves into their
own module.

Intended result. `git ls-files docs/` lists both documents, and every path that
DESIGN-V2.md cites exists in a fresh clone.

### T2. Settle the open decisions

Status: done on 2026-09-29. The maintainer answered every question and
follow-up in #23, the answers are recorded in DESIGN-V2.md, and no item there is
open or proposed.

Motivation. DESIGN-V2.md lists eight open decisions and a number of proposals.
Each open decision blocks a named task; building on an unconfirmed proposal
means rework when it is rejected.

Work. Decide each item in DESIGN-V2.md, "Open decisions". Confirm or reject each
`[Proposed]` item. Change the markers in DESIGN-V2.md accordingly.

Intended result. No `[Open]` or `[Proposed]` item remains that a phase 1 task
depends on. Items that only affect phase 2 may stay open until T14 starts.

## Phase 1: reading

### T3. Rename to the device agent

Motivation. The repository is skuhus/device-agent, but the module path, binary,
environment prefix, directories and image still carry the scanner's name
(go.mod line 1; internal/config/load.go:17; Makefile lines 8-9). Renaming first
means every later pull request uses the final names, and the rename can be
reviewed as one mechanical diff.

Work.
- Module path to github.com/skuhus/device-agent, and every import with it.
- Binary, cmd/ directory, configuration directory, log directory, image name and
  the account inside the image, to skuhus-device-agent; the environment prefix to
  SH_DEV_AGENT_.
- Makefile, Dockerfile, .github/workflows/ci.yml and release.yml, README.md,
  config.sample.yaml, dev/agent.local.yaml.
- `git remote set-url origin git@github.com:skuhus/device-agent.git` in each
  working copy (local, not a commit).
- No behaviour change in this pull request.

Intended result. A search for device-serial-scanner and SH_DEV_SER_SCANNER finds
them only in DESIGN.md, the v1 record that T12 retires; in DESIGN-V2.md and
PLAN-V2.md where they cite v1 files at commit f97c736; and in
device-agent-spec.md, which is the maintainer's document and which T3 does not
edit. `make check` passes. The image's
`version` subcommand prints the new name. The asset names produced by the
release workflow's build step use the new name.

Depends on: T1, T2.

### T4. Give spike/ and dev/ their own modules

Motivation. spike/mqtt5, spike/brokerinfo and dev/consumer are tools, not the
agent, but they are packages of the agent's module. `go vet ./...` and `go test
./...` compile them in CI, in the release workflow and in `make check`, so a
tool that stops compiling, for example after a paho.golang update, fails those
checks and blocks a release. They add no dependency: all three import only
paho.golang, which the agent's transport imports too. DESIGN-V2.md,
"Implementation approach", leaves both directories out of the agent's module.

Work.
- Fix #25: the spike leaves retained messages behind on failure paths, and has
  names damaged by a renaming pass.
- Add a go.mod in spike/ and in dev/, making each a nested module. `./...` in
  the repository root does not match a nested module's packages.
- Run the Makefile's spike targets and `consume` inside their modules, for
  example `go -C spike run ./mqtt5`. `go run ./spike/mqtt5` from the root fails
  once spike/ is its own module.
- gofmt keeps checking both directories, because `gofmt -l .` walks directories
  rather than modules. `go vet`, `go test` and the tidy check stop covering
  them, and nothing checks the nested go.sum files.

Intended result. `go list ./...` in the repository root lists no package under
spike/ or dev/. A compile error in spike/ or dev/ no longer fails `go vet ./...`
or `go test ./...` in the root. `go mod tidy` in the root leaves go.mod and
go.sum unchanged. `make spike-brokerinfo`, `make spike-mqtt5` and `make consume`
run against the development broker.

Depends on: T1, T3.

### T5. Configuration schema v2

Motivation. The v1 schema is shaped around a scanner:
- one delivery.scan_ttl for the whole process (internal/config/config.go:114);
- assert_config, which is accepted and then only produces a warning that it is
  not implemented (internal/device/serial/serial.go:212);
- device ids that may contain characters a topic segment may not
  (internal/config/validate.go:27 against validate.go:24);
- serial framing fixed at 8 data bits, no parity, one stop bit in
  internal/device/serial/serial.go, New, where the maintainer asked for the port
  parameters to be configurable.

Work. Per device: id (topic-segment rule, with `agent` reserved), path, baud,
data bits, parity, stop bits, separator, max_frame_bytes, inter_char_timeout,
message expiry, and an optional device_type string. Remove assert_config.
Replace audit_file, audit_max_size_mb and audit_keep with the common log's keys,
logging.file, logging.max_size_mb and logging.keep, and add logging.stdout; each
destination is optional, and a configuration with neither is accepted
(DESIGN-V2.md, "Logging: one common log"; #23 Q11, Q11a). Framing without a separator is #7 and
not part of this task.

Keep: strict loading of keys and environment variables; validation that reports
every problem at once; the credential rules; every check listed in DESIGN.md,
"Checks that exist because of a specific failure". Rewrite config.sample.yaml.

Intended result. A table-driven test for every rejection, asserting the message
text. config.sample.yaml validates. A v1 configuration file is rejected by strict
loading with each key that no longer exists named, so an operator upgrading a
station learns what changed from the error, not from a silent default.

Depends on: T2, T3.

### T6. Topics and message formats

Motivation. The topics and the JSON messages are the contract that parsers,
relays and the tx senders are built against. Writing them down before the code
lets those services start in parallel and makes the contract reviewable on its
own.

Work.
- Add a "Message formats" section to DESIGN-V2.md with a complete JSON example
  of each message: rx, tx (reserved in phase 1), device status event, tx result,
  agent keepalive with counters, will. Every field, its type, and whether it can
  be null.
- In the same section, state the tx contract: the id is a UUID of any version,
  and a sender that has no result after a stated time treats the tx as not
  written, because a tx published while the agent is disconnected is lost
  (#23 Q15a, Q18, Q19a).
- Implement the topic builder with the device segment and the agent/<instance>
  level, rejecting `agent` as a device id.
- Implement the message types, with tests that assert the exact set of JSON
  fields, following internal/event/status_test.go and heartbeat_test.go.

Intended result. The format section exists and is reviewed. The tests fail if a
field is added, removed or renamed without the section changing. A wildcard
subscription to `skuhus/<project>/<site>/<station>/+/rx` receives messages from
two devices on the development broker.

Depends on: T2.

### T7. Core: reading and publishing

Motivation. This is the part of v1 the maintainer judged easier to rewrite than
to fix. The supervisor in internal/agent owns publishing, status, heartbeat,
presence and the drain policy in one type. runAgent
(cmd/skuhus-device-serial-scanner/run.go:90-243) assembles logging, audit,
credentials, devices, transport and supervisor in one 154-line function.
Validation is repeated in agent.New.

Work. A new core with one responsibility per part:
- Port reader: open, read, frame, reopen with backoff. Carry the framer, its
  tests and the captures in internal/device/serial/testdata unchanged.
- Per-device pipeline: reader to bounded channel to publisher, publishing to
  that device's rx topic.
- Transport: carry the connection handling from internal/transport/mqtt, whose
  properties were each measured: the connection outlives the run context, the
  disconnect is bounded, there is no publish queue, and the connection is safe
  for concurrent publishing (autopaho wraps it in packets.NewThreadSafeConn).
- Delivery outcomes: a record in the common log for every message, with the
  payload when the broker did not accept it.
- Wiring that assembles the parts and does nothing else; validation in the
  configuration package only.
- An absent port keeps the agent running: it is reported on the device status
  topic and counted, and opening is retried with backoff.

Carry the v1 tests that encode a measured failure, rewritten against the new
core: shutdown order (devices, drain, offline status, disconnect), the drain
deadline, the bounded disconnect, a failed publish being counted and recorded
with its payload, and the pseudo-terminal harness tests.

Intended result. With the pseudo-terminal harness, every frame produces exactly
one rx message on its device's topic. Two devices on one agent publish to their
own topics. The carried tests pass. A publish that fails leaves its payload in
the common log.

Depends on: T5, T6.

### T8. Status channel and counters

Motivation. The maintainer wants device problems visible outside the host, and
every agent and device visible from outside (DESIGN-V2.md, "Status channel").
v1 publishes a heartbeat with some counters and no per-device events.

Work.
- Agent keepalive with the per-device counters listed in DESIGN-V2.md.
- Device events on the device status topic: opened, closed, lost with its error
  class, discard with reason and byte count.
- The will on the agent-level status topic.
- Consumers derive liveness from the keepalive; RabbitMQ does not retain a will
  (docs/spikes/m0-mqtt5.md:106). Say so in the message format section.

Intended result, each checked end to end against the development broker:
- Closing the pseudo-terminal publishes a port-lost event with its error class.
- A device configured with a separator it never sends shows timeout discards
  rising and rx frames at zero in consecutive keepalives: the inverted-separator
  signature, visible without reading a log on the station.
- Killing the agent with SIGKILL publishes the will.

Depends on: T6, T7.

### T9. Common log

Motivation. Two maintainer decisions. On 2026-09-29: one common log that
everything is written to, replacing the separate audit log, with every record
identifying where it was produced. On 2026-09-07, as a defect: the data read from
the port only reaches the log at DEBUG (internal/device/serial/serial.go:281),
so seeing a reading means enabling every other DEBUG line as well.

Work, as DESIGN-V2.md, "Logging: one common log", describes it:
- One log, written to a file with size rotation, to stdout, or both, as
  configured.
- Every record carries its severity and slog's source attribute (function,
  file, line).
- Delivery outcomes are ordinary records, with the payload when the broker did
  not accept the message.
- Records at INFO and above are flushed to disk as written.
- With log_payloads set, the INFO line for each frame carries its data, as hex
  and as text when it is valid UTF-8. Discard warnings carry reason and byte
  count, and the discarded data when log_payloads is set.
- One line per event, and no key repeated within a line. v1 had one such line:
  the topics line carried instance_id twice until it was fixed.

Intended result. Tests that capture the log and assert: the frame line's content
with log_payloads set and unset; source present on every record; a
failed publish's payload present; no repeated key in any line. Rotation keeps
the configured number of files.

Depends on: T5, T7.

### T10. End-to-end test in CI

Motivation. The worst v1 defects were found only by running the agent against a
broker by hand: the connection was torn down before the offline status could be
sent (DESIGN.md, "The broker connection outlives the run context"), and a
failed scan left nothing but its length in the audit log. The integration job
the specification asks for (section 11) was never built.

Work.
- An integration test, behind the build tag `integration`, that starts the
  pseudo-terminal device and a RabbitMQ 4.x broker, runs the agent, subscribes,
  and asserts what arrives.
- A CI job that runs it. Start the broker in a step after checkout with
  `docker run`, mounting dev/rabbitmq/: GitHub service containers start before
  checkout and cannot mount files from the repository, and the broker needs its
  plugin list and definitions.

Intended result. The CI job passes and covers: frames published to the right
topics; device events; the will after SIGKILL; a failed publish recorded in the
common log with its payload when the broker is stopped mid-run.

Depends on: T7, T8, T11.

### T11. Development environment for v2

Motivation. The development broker's permissions and the consumer are written
for v1: the ingest user may only write to topics ending in `.cmd`
(dev/rabbitmq/definitions.json:64), and the consumer knows the v1 message
shapes.

Work.
- definitions.json: the station user reads and writes its own station's tree
  including the device segment; the ingest user may write `.tx`. Test the
  station user's existing pattern against device-level topics rather than
  assuming it matches.
- dev/consumer prints rx, tx and status, decoding the payload. Fix #24 on the
  way: it keeps running silently after a connection error, and has a dead
  loop.
- dev/agent.local.yaml in the v2 schema.

Intended result. `make broker-up` and `make consume` show a v2 agent's rx and
status messages. A station user cannot publish to another station's tx topic,
checked the way M0 checked topic permissions (docs/spikes/m0-mqtt5.md,
"Reproducing").

Depends on: T5, T6.

### T12. Documentation

Motivation. After phase 1, DESIGN.md describes topics and a supervisor that no
longer exist, and README.md describes a scanner. Two design documents would
disagree about the same product. Separately, AGENTS.md says the commit subject
format lives in a README section called "Github naming convention", and README
has no such section.

Work.
- Rewrite README.md for the device agent: running, configuration, container,
  topics, CI and release.
- config.sample.yaml as T5 left it.
- Move into DESIGN-V2.md the reasoning it currently borrows from DESIGN.md, then
  remove DESIGN.md, leaving one design document.
- Add the "Github naming convention" section AGENTS.md refers to.

Intended result. Every citation in DESIGN-V2.md resolves inside the tree. No
document describes the scan, cmd or heartbeat topics.

Depends on: T3 to T11.

### T13. Release 2.0.0

Work.
- Set the version constant to 2.0.0 in the pull request that completes phase 1.
- The release pushes the image to GHCR, as decided; check that it does, under
  the new name.
- Check that the release assets carry the new binary name.

Intended result. The merge creates tag v2.0.0 and a release with six tarballs,
six bare binaries and checksums.txt, and the image appears on GHCR.

Depends on: T2 to T12.

## Phase 2: writing

### T14. tx: receive and write

Motivation. Printers are the main reason for writing (DESIGN-V2.md, "Scope").

Two properties of go.bug.st/serial v1.8.0 shape this task. Its Write does not
take the lock that Read and Close use and does not check whether the port is
open (serial_unix.go:112-118, against Read at 59-61 and Close at 48-49). A write
racing a close therefore reaches whatever descriptor now has that number. It
also returns the result of a single write(2) and does not continue after a
partial write.

Work.
- Subscribe to each device's tx topic. Validate the message, including that its
  id is a UUID; a malformed tx gets a failed result with a code and a text.
- One writer per port, which only writes while it holds the port open under the
  agent's own lock, and loops until every byte is written.
- Results: accepted, then written or failed, each with a code, a text and the
  detail at hand: the port's error class, bytes written, attempts made.
- Retry opening the port, up to the configured count and at the configured
  interval, then fail the tx. Once writing has started, a failure is not
  retried: the result reports the bytes written (#23 Q17, Q17a).
- No gap between writes and no read or write priority: the line is full-duplex
  and reading continues throughout (#23 Q16a).
- Decide whether "written" is reported after write(2) accepts the bytes or after
  Drain (the library exposes tcdrain as Port.Drain) confirms they left the
  buffer. The maintainer's definition, bytes written to the port, fits either;
  Drain is what makes it true of the wire.
- Record tx outcomes in the common log, with the payload for failures.
- Reading continues while writing.

Intended result, with the pseudo-terminal harness: bytes arrive at the other end
intact; many concurrent tx messages arrive as whole, uninterleaved blocks; a tx
to an absent port fails with its error class; every tx id gets a result; a write
racing a close fails and never reaches another descriptor.

Depends on: T13.

### T15. Postponed: tx through reconnects

Postponed out of #4 on 2026-09-29 (#23 Q19a). This task's issue, #20, now holds
it as a feature. Until then rx and tx share one clean connection, a tx
published while the agent is disconnected is lost, and the sender learns it from
the missing result; T6 writes that rule into the message formats. The number is
kept so that references to T15 stay unambiguous.

### T16. tx idempotency

Work. Keep a bounded set of recently written tx ids. A tx whose id is in the set
is not written, and gets a result saying it was a duplicate. The set lives in
memory; a restart forgets it, and the message format section says so.

Intended result. The same id sent twice produces one write and two results.

Depends on: T14.

### T17. Moved to #5

Flow control and write pacing were postponed out of #4 on 2026-09-29. #5 records
why go.bug.st/serial cannot provide them, the alternatives evaluated, and the
proposed replacement. The number is kept so that references to T17 stay
unambiguous.

### T18. Release 2.1.0

Intended result. Tag v2.1.0, releasing writing.

Depends on: T14, T16.

## Outside #4

Recorded so they are not lost:

- #5: flow control and write pacing.
- #6: reloading the configuration on SIGHUP.
- #7: framing for devices that send no separator.
- #20: keeping tx through reconnects on a persistent session (formerly T15).
- #26: the same counters on an HTTP endpoint for scraping.
- #27: restricting tx ids to UUID version 4.

- Exchange sessions and polling (DESIGN-V2.md, "Deferred beyond #4").
- Port locking between processes (same).
- macOS: Developer ID signing and notarisation. notarytool accepts .zip, .pkg
  and .dmg, not .tar.gz, and only a .pkg or .dmg can be stapled.
- Packaging: systemd unit, udev rules including the ModemManager ignore,
  launchd plist, deb and rpm.
- golangci-lint in CI.
