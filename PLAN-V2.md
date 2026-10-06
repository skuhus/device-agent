# Plan v2: subtasks of #4

The work that turns the scanner agent into the device agent described in
DESIGN-V2.md. Each task is sized for one pull request, except where it says
otherwise.

Task numbers are referenced from DESIGN-V2.md, "Open decisions". Decision
markers ([Decided], [Proposed], [Open]) have the meanings given there. File
references are to commit f97c736.

Phase 1 delivers reading and phase 2 writing, and both are released together
as 2.0.0 (T13). Decided by the maintainer on 2026-10-04, replacing the decision
of 2026-09-29 to release reading as 2.0.0 and writing as 2.1.0.

Postponed out of #4 into their own tickets: #5 flow control and write pacing
(formerly T17), #6 reloading the configuration on SIGHUP, #7 framing for devices
that send no separator.

## Tracking

Every task is a sub-issue of #4, in this order: the order the tasks can be done
in, each after the tasks it depends on. T11 therefore comes before T10, and T13
after T16. T1 was finished before issues
were created for the plan, so it has none. #6 and #7 are sub-issues of #4 too,
postponed beyond it, as are #20 (formerly T15), #26, #27 and #28. #24 and #25 are
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
| T11 | #16 | Development environment for v2 |
| T10 | #15 | End-to-end test in CI |
| T12 | #17 | Documentation |
| T14 | #19 | tx: receive and write |
| T15 | #20 | tx through reconnects, postponed out of #4 |
| T16 | #21 | tx idempotency |
| T17 | #5 | Flow control and write pacing, postponed out of #4 |
| T13 | #18 | Release 2.0.0 |
| T18 | #22 | Release 2.1.0, merged into T13 |

## Order

    T1 commit docs ----+
    T2 decisions ------+--> T3 rename --> T4 tool modules
                                |
                                +--> T5 config --+
                                +--> T6 messages +--> T7 core --> T8 status --+
                                                 |           +--> T9 logging  |
                                                 +--> T11 dev env ------------+--> T10 e2e --> T12 docs --> T14 tx --> T16 tx idempotency --> T13 release 2.0.0

T3 goes first among the code tasks so that every later pull request is written
against the final names, and the rename is a single diff with no behaviour
change in it.

## Phase 0: before code

### T1. Commit the measurements in docs/

Status: done on branch gh-4-docs, merged to master in #31. It was committed
at the maintainer's request on 2026-09-29, together with dev/, spike/ and these
two documents.

Motivation. DESIGN-V2.md cites docs/spikes/m0-mqtt5.md for every broker
constraint and docs/scanners/symbol-05e0-1701.md for the signature of an
unmatched separator. Neither file is tracked: at f97c736, `git status` shows
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
follow-up in #23, the answers are recorded in DESIGN-V2.md, and when T2 closed
no item there was open or proposed. T6 then proposed the message formats and
the instance rule, which the maintainer settled in #11 the same day.

Motivation. DESIGN-V2.md lists eight open decisions and a number of proposals.
Each open decision blocks a named task; building on an unconfirmed proposal
means rework when it is rejected.

Work. Decide each item in DESIGN-V2.md, "Open decisions". Confirm or reject each
`[Proposed]` item. Change the markers in DESIGN-V2.md accordingly.

Intended result. No `[Open]` or `[Proposed]` item remains that a phase 1 task
depends on. Items that only affect phase 2 may stay open until T14 starts.

## Phase 1: reading

### T3. Rename to the device agent

Status: done on branch gh-8-rename, merged to master in #31.

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

Status: done on branch gh-9-tool-modules, merged to master in #31.

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

Status: done on branch gh-10-config, merged to master in #31. The v1 binary runs
on the v2 schema until T7 and T9 replace its wiring.

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
not part of this task. Check identity.instance against the topic-level rule
`[a-z0-9-]+`, since the instance is now a topic level (DESIGN-V2.md, "Topics";
#11 Q2); v1 allows `[A-Za-z0-9._-]{1,64}`. Add the
keepalive interval, default 15 s, and the number of missed keepalives after
which a consumer treats the agent as gone, default 3 (#11 Q7).

Keep: strict loading of keys and environment variables; validation that reports
every problem at once; the credential rules; every check listed in DESIGN-V2.md,
"Checks that exist because of a specific failure". Rewrite config.sample.yaml.

Intended result. A table-driven test for every rejection, asserting the message
text. config.sample.yaml validates. A v1 configuration file is rejected by strict
loading with each key that no longer exists named, so an operator upgrading a
station learns what changed from the error, not from a silent default.

Depends on: T2, T3.

### T6. Topics and message formats

Status: done on branch gh-11-message-formats, merged to master in #31; the
maintainer reviewed the format section in #11.

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

Depends on: T2, T3.

### T7. Core: reading and publishing

Status: done on branch gh-12-core, merged to master in #31. Until T8 the agent
publishes rx and its offline message and will, but no keepalive or device
events.

Motivation. This is the part of v1 the maintainer judged easier to rewrite than
to fix. The supervisor in internal/agent owns publishing, status, heartbeat,
presence and the drain policy in one type. runAgent
(cmd/skuhus-device-serial-scanner/run.go:90-243) assembles logging, audit,
credentials, devices, transport and supervisor in one 154-line function.
Validation is repeated in agent.New.

Work. A new core with one responsibility per part:
- Port reader: open, read, frame, reopen with backoff. Carry the framer, its
  tests and the captures in internal/device/serial/testdata unchanged. The
  reader reports what happens to its port: opened, closed, lost and a failed
  open, the last two with their error class, and discarded bytes with reason
  and count. The core logs each; T8 publishes and counts them.
- Per-device pipeline: reader to bounded channel to publisher, publishing to
  that device's rx topic with the device's message expiry.
- Transport: carry the connection handling from internal/transport/mqtt, whose
  properties were each measured: the connection outlives the run context, the
  disconnect is bounded, there is no publish queue, and the connection is safe
  for concurrent publishing (autopaho wraps it in packets.NewThreadSafeConn).
  It publishes rx and the offline message as DESIGN-V2.md, "Message formats",
  says, and registers the will; T8 adds keepalive and events.
- The will and the offline message on the agent's status topic. Both are part
  of the connection's life, which this task builds: the will goes in the
  CONNECT packet, and the offline message is part of shutdown.
- Delivery outcomes: a record in the log file for every rx message, with the
  payload when the broker did not accept it. T9 makes that file the common log.
- Wiring that assembles the parts and does nothing else; validation in the
  configuration package only.
- An absent port keeps the agent running, and opening is retried with backoff;
  T8 reports it on the device status topic and counts it.
- Remove what the new core replaces: the v1 supervisor, the v1 messages in
  internal/event, and the v1 topics.

Carry the v1 tests that encode a measured failure, rewritten against the new
core: shutdown order (devices, drain, offline message, disconnect), the drain
deadline, the bounded disconnect, a failed publish recorded with its payload,
and the pseudo-terminal harness tests.

Intended result. With the pseudo-terminal harness, every frame produces exactly
one rx message on its device's topic. Two devices on one agent publish to their
own topics. The carried tests pass. A publish that fails leaves its payload in
the log file. Killing the agent with SIGKILL makes the broker publish its will,
checked against the development broker.

Depends on: T5, T6.

### T8. Status channel and counters

Status: done on branch gh-13-status, merged to master in #31.

Motivation. The maintainer wants device problems visible outside the host, and
every agent and device visible from outside (DESIGN-V2.md, "Status channel").
v1 publishes a heartbeat with some counters and no per-device events.

Work.
- Agent keepalive with the per-device counters listed in DESIGN-V2.md, at the
  configured interval, carrying `gone_after_s` (#11 Q7).
- Device events on the device status topic: opened, closed, lost with its error
  class, a failed open on every attempt (#11 Q8), discarded bytes with reason
  and byte count, from the port events T7's reader reports.
- Events that happen while the broker connection is down wait for it, each
  device keeping only its most recent `status.event_buffer_size` (#13 Q1).
- Count, per device, what the keepalive reports.

Intended result, each checked end to end against the development broker:
- Closing the pseudo-terminal publishes a port-lost event with its error class.
- A device whose separator never appears in what it sends, misconfigured or
  missing from the data, shows timeout discards rising and rx frames at zero in
  consecutive keepalives: the unmatched-separator signature, visible without
  reading a log on the station.

Depends on: T6, T7.

### T9. Common log

Status: done on branch gh-14-common-log, merged to master in #31.

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

### T11. Development environment for v2

Status: done on branch gh-16-dev-env, merged to master in #31. A broker already
running keeps the old definitions until it restarts (`make broker-down
broker-up`).

Motivation. The development broker still grants v1's command topic: the ingest
user may write only to topics ending in `.cmd`
(dev/rabbitmq/definitions.json:64). v2 has no such topic; senders publish tx
instead (DESIGN-V2.md, "tx"), and the ingest user stands in for them. Measured
on RabbitMQ 4.3.5: its publish to a tx topic is refused, and the broker closes
the connection. dev/consumer carries two defects (#24) and v1's wording. It
prints any JSON payload and decodes `raw_b64`, so it shows v2's messages as it
is.

The station user's topic permission already covers its whole station tree, the
device level included: `make test-broker` publishes rx, a device event and a
keepalive through it (#11), and the agent published rx, events, keepalives, its
offline message and its will through it in T7 and T8. It stays as it is.

Work.
- definitions.json: the ingest user may write device tx topics,
  `skuhus/<project>/<site>/<station>/<device>/tx`, in place of `.cmd`.
- `make test-broker` checks the permissions senders and stations rely on: the
  ingest user can publish to a device's tx topic and cannot publish to an rx
  topic or to a device's or an agent's status topic, and a station user cannot
  publish to another station's tx topic. RabbitMQ refuses a publish by closing
  the connection, which the client does not notice while it waits for the
  acknowledgement, so a check sees a refusal as a publish never acknowledged
  and a message never delivered.
- dev/consumer: fix #24, and drop v1's wording.
- dev/agent.local.yaml: its log_payloads comment says the data needs level
  debug, which stopped being true with T9.

The broker imports the definitions when it boots, so a running broker takes the
change at its next restart.

Intended result. `make test-broker` passes on a broker started from the new
definitions, and each permission check fails when the permission it checks is
widened or removed. With the agent on the pseudo-terminal harness, `make
consume` shows its rx, device events, keepalives and offline message. Closing
the broker's side of the connection ends `make consume` with the error printed.

Depends on: T5, T6.

### T10. End-to-end test in CI

Status: done on branch gh-15-e2e, merged to master in #31. `make
test-integration` passes against the development broker; the CI job runs once
the branch is pushed.

Motivation. The worst v1 defects were found only by running the agent against a
broker by hand. On SIGTERM the connection was torn down before the offline
status could be sent, and the broker published the will instead
(DESIGN-V2.md, "The broker connection outlives the run context"). A failed scan
left nothing but its length in the audit log, until f97c736 (gh-1). The
integration job the specification asks for (section 11) was never built.

Work.
- An integration test, behind the build tag `integration`, that runs the
  agent's binary against a pseudo-terminal device and the RabbitMQ broker the
  environment names, as `make test-broker` names one, subscribes, and asserts
  what arrives. The agent reaches the broker through a relay inside the test,
  so that the test can take the broker away mid-run without stopping a broker
  someone else is using.
- `make test-integration`, which vets the test with its tag and runs it against
  the development broker.
- A CI job that runs `make broker-up` and `make test-integration`. The broker is
  started in a step after checkout: GitHub service containers start before
  checkout and cannot mount files from the repository, and the broker needs its
  plugin list and definitions from dev/rabbitmq/. release.yml runs the same
  gates before it tags, so it runs the integration test too.

Intended result. The test passes against the development broker, and covers:
- every frame published once on its device's rx topic, byte-exact, with the
  device's message expiry;
- device events: `port_opened`, and `port_lost` when the pseudo-terminal
  closes;
- a keepalive carrying the device's counters;
- on SIGTERM, the offline message with reason `shutdown`, and no will;
- on SIGKILL, the will;
- with the broker taken away mid-run, every reading recorded in the common log
  as failed, with its data.

The CI job passes on the pull request, which is seen once the branch is pushed.

Depends on: T7, T8, T9, T11.

### T12. Documentation

Status: done on branch gh-17-docs, merged to master in #31.
docs/spikes/m0-mqtt5.md also named the v1 topics as the agent's, and now says it
is a record of v1's.

Motivation. After phase 1, DESIGN.md describes topics and a supervisor that no
longer exist, and README.md describes the v1 scanner agent: a retained status,
a heartbeat, an audit log and the scan topic. Two design documents would
disagree about the same product. DESIGN-V2.md borrows the reasoning for its
carried-over decisions from DESIGN.md: 22 of DESIGN.md's headings are cited in
the tree, all of them from DESIGN-V2.md.

Work.
- Rewrite README.md for the device agent as built: what it is, building and
  testing, the development broker, running, configuration, the container,
  topics and messages, CI and release, field diagnosis. Keep what is still
  true, such as the GitHub naming convention and the environment hazards, and
  the headings DESIGN-V2.md cites: "Continuous integration" and "Container".
- Move into DESIGN-V2.md the reasoning of every DESIGN.md heading cited in the
  tree, under the same heading where it still holds, and say what v2 changed
  where it does not. Then remove DESIGN.md, leaving one design document.
- Point every citation of DESIGN.md at its new place: in DESIGN-V2.md,
  PLAN-V2.md, README.md, config.sample.yaml and code comments. Two files keep
  theirs. internal/config/testdata/config-0.3.0.yaml is the 0.3.0 sample as
  shipped, and a test holds it unchanged. device-agent-spec.md is the
  maintainer's document, which this work does not edit (T3).

Intended result.
- Every citation in DESIGN-V2.md resolves: a section inside the tree, and a v1
  file at commit f97c736, as the document's convention says.
- Outside the two files named above, nothing cites DESIGN.md as a place to
  read.
- No document describes the scan, cmd or heartbeat topics as the agent's, except
  device-agent-spec.md; DESIGN-V2.md and PLAN-V2.md name them only as what v2
  replaced.
- Every make target, subcommand, flag, configuration key and environment
  variable that README.md names exists.

Depends on: T3 to T11.

### T13. Release 2.0.0

Status: released as v2.0.0 from #32 (d112631) on 2026-10-05; the checks after
the merge are on #18. Open there: the image package is still private, so the
anonymous pull is not checked yet (an anonymous token request was refused on
2026-10-06), and README.md does not say that a downloaded bare binary needs
`chmod +x`.

2.0.0 releases reading and writing together (maintainer, 2026-10-04). Phase 2
merged to master in #31, so this task's pull request carries the release alone.

Work.
- Set the version constant to 2.0.0, with the sentence in DESIGN-V2.md,
  "Version: 2.0.0, and 1.0.0 is never used", that names the constant.
- Before the merge, run the release workflow's build step locally and check
  what it makes: six tarballs and six bare binaries named
  `skuhus-device-agent-2.0.0-<os>-<arch>`, each tarball holding the binary,
  config.sample.yaml, README.md and LICENSE, and checksums.txt over the twelve.
- After the merge, check the release: the tag v2.0.0 on the merge commit, the
  twelve assets and checksums.txt, a downloaded binary that matches
  checksums.txt and reports 2.0.0, and the image
  `ghcr.io/skuhus/skuhus-device-agent` at 2.0.0 and latest on GHCR, reporting
  2.0.0. The release pushes the image to GHCR, as decided.
- README.md gains "Upgrading", with what changed from 0.3.0 to 2.0.0, and the
  release notes start with a link to it, above the generated notes (#18 Q1).
- The image is public (#18 Q2). After the release first pushes it, an
  organisation owner makes the package public; then an anonymous pull is
  checked.

Intended result. The merge creates tag v2.0.0 and a release with six tarballs,
six bare binaries and checksums.txt, and the image appears on GHCR.

Depends on: T2 to T12, T14, T16, all merged in #31.

## Phase 2: writing

### T14. tx: receive and write

Status: done on branch gh-19-tx, merged to master in #31. A tx the agent stops
before writing fails as `agent_stopping` (#19 Q1).

Motivation. Printers are the main reason for writing (DESIGN-V2.md, "Scope"),
and 2.0.0 now releases writing (DESIGN-V2.md, "Version"). The bench printer is
an Epson TM-T20III on RS-232 through an ATEN UC-232A adapter (PL2303), at 9600
baud 8N1, measured on 2026-10-04 (docs/printers/epson-tm-t20iii.md).

Two properties of go.bug.st/serial v1.8.0 shape this task. Its Write does not
take the lock that Read and Close use and does not check whether the port is
open (serial_unix.go:112-118, against Read at 59-62 and Close at 48-49). A write
racing a close therefore reaches whatever descriptor now has that number. It
also returns the result of a single write(2) and does not continue after a
partial write.

Two measurements on the bench printer settle what T14 left open:
- `written` means the operating system accepted every byte, as DESIGN-V2.md,
  "Writing: tx", decides. Drain is not used: on macOS with the PL2303 it
  returned 2 ms after an 8281-byte job that needs 8.6 s on the line, so it does
  not say the bytes left the wire either.
- Flow control stays postponed (#5). The printer signalled nothing on DSR, CTS
  or with XON/XOFF, and a 200-line job printed whole.

Work.
- Subscribe to each device's tx topic at QoS 1 on every connection, since each
  connection starts clean and keeps no subscription. Validate the message,
  including that its id is a UUID; a malformed tx gets a failed result with a
  code and a text.
- One writer per port. The device's reader keeps owning the port, as now, and
  the writer writes through the port the reader has open, under a lock that
  Close also takes. It writes in chunks, taking the lock for each, so that a
  close waits for one chunk rather than the whole tx, and loops until every
  byte is written.
- A tx that finds the port closed asks the reader to try opening it now,
  rather than after its backoff, and counts that as an attempt. Two device
  settings bound this: `tx_open_attempts`, default 3, and `tx_open_interval`,
  default 1 s. Once writing has started, a failure is not retried: the result
  reports the bytes written (#23 Q17, Q17a).
- No attempt once the tx's message expiry has passed, taken from the MQTT
  message expiry it was delivered with (#23 Q19); that tx gets `expired`. A tx
  delivered without an expiry has no such limit.
- Results as DESIGN-V2.md, "Message formats", lists them, each with its code,
  text and detail keys. They go out through the device's event queue, so they
  wait for the connection within the device's message expiry, as its events do
  (#13 Q1), and keep their order.
- A tx whose id belongs to a tx still queued or being written is rejected with
  `in_progress`, saying where that one stands and since when, and is not queued
  (#11 Q6). Until #28, this is also how a sender asks about a tx (#11 Q5).
- The keepalive's `tx_written` counts `written` results, and `tx_failed` counts
  `failed` ones.
- No gap between writes and no read or write priority: the line is full-duplex
  and reading continues throughout (#23 Q16a).
- Record every tx outcome in the common log, as a reading's is: the data on a
  failure always, and on a written tx with log_payloads.
- A development tool that publishes a tx as `ingest`, `make send-tx`, so that
  writing can be watched with `make consume` as reading can.

Intended result.
- With the pseudo-terminal harness:
  - bytes arrive at the other end intact;
  - many concurrent tx messages arrive as whole, uninterleaved blocks;
  - a tx to an absent port fails with its error class after
    `tx_open_attempts`;
  - every tx id gets a result;
  - a write racing a close fails and never reaches another descriptor.
- The end-to-end test publishes a tx through the broker and checks the bytes at
  the pseudo-terminal and the `accepted` and `written` results.
- On the bench, a job sent with `make send-tx` prints whole on the
  TM-T20III, and gets `accepted`, then `written`.

Depends on: T7 to T12.

### T15. Postponed: tx through reconnects

Postponed out of #4 on 2026-09-29 (#23 Q19a). This task's issue, #20, now holds
it as a feature. Until then rx and tx share one clean connection, a tx
published while the agent is disconnected is lost, and the sender learns it from
the missing result; T6 writes that rule into the message formats. The number is
kept so that references to T15 stay unambiguous.

### T16. tx idempotency

Status: done on branch gh-21-idempotency, merged to master in #31.

Motivation. A sender that hears nothing within its own timeout resends, and a
resend after the first copy was written would print it twice (#11 Q6a). T14
already answers a resend while the first copy is queued or being written, with
`in_progress`; T16 covers the time after it was written.

Work.
- Each device remembers, in memory, the ids of its most recent 1024 written
  tx and when each was written; a restart forgets them (#11 Q6a). The number
  is an internal constant, like the drain timeout: a sender resends within
  seconds or minutes, and 1024 jobs is more than any station prints in that
  time. (#30 made both settings: `devices[].tx_remembered_ids` and
  `delivery.drain_timeout`, with these values as defaults.)
- Only a tx that got `written` is remembered. One that failed, part way or not
  at all, is written again if it is sent again: its sender was told what
  became of it.
- A tx whose id is remembered is not queued. It gets `written` /
  `already_written` alone, at once, with `written_at` (DESIGN-V2.md, "tx
  results"), as a resend in progress gets `in_progress` alone.
- The keepalive's `tx_written` does not count it, since nothing was written.
- Record it in the log, with its data, as every tx outcome is.

Intended result.
- A resend after `written` gets `already_written` with the first copy's
  `written_at`, and nothing more is written; a resend after `write_failed` is
  written again; the 1025th written id pushes out the oldest; a new agent
  knows no id.
- The end-to-end test sends one tx twice through the broker: it reaches the
  pseudo-terminal once, and the second copy gets `already_written`.
- On the bench, a job sent twice with one id prints once.

Depends on: T14.

### T17. Moved to #5

Flow control and write pacing were postponed out of #4 on 2026-09-29. #5 records
why go.bug.st/serial cannot provide them, the alternatives evaluated, and the
proposed replacement. The number is kept so that references to T17 stay
unambiguous.

### T18. Merged into T13

T18 was to release writing as 2.1.0. On 2026-10-04 the maintainer decided that
reading and writing are released together as 2.0.0, so T13 releases both. The
number is kept so that references to T18 stay unambiguous.

## Outside #4

Recorded so they are not lost:

- #5: flow control and write pacing.
- #6: reloading the configuration on SIGHUP.
- #7: framing for devices that send no separator.
- #20: keeping tx through reconnects on a persistent session (formerly T15).
- #26: the same counters on an HTTP endpoint for scraping.
- #27: restricting tx ids to UUID version 4.
- #28: asking the agent where a tx stands; until then a resend with the same id
  is the enquiry (#11 Q5).
- #36: describing a device beyond `device_type`: its model, description, serial
  and part number.

- Exchange sessions and polling (DESIGN-V2.md, "Deferred beyond #4").
- Port locking between processes (same).
- macOS: Developer ID signing and notarisation. notarytool accepts .zip, .pkg
  and .dmg, not .tar.gz, and only a .pkg or .dmg can be stapled.
- Packaging: systemd unit, udev rules including the ModemManager ignore,
  launchd plist, deb and rpm.
- golangci-lint in CI.
