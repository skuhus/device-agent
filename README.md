# device-agent

Device agent that gives network access to devices physically attached to a host.
It reads bytes from a serial port, USB-CDC or RS-232, and publishes each frame to
an MQTT broker. Writing bytes received from MQTT to the port comes in 2.1.0.

It is a transport: it moves bytes and adds an envelope. It implements no device
protocol; parsing, interpretation and relaying are done by services that
subscribe to what it publishes. It does not do offline sync: a reading the
broker did not take is recorded in the log, not sent later.

`DESIGN-V2.md` is the design of the device agent: what v2 changed from v1, the
scanner agent, and why, and the v1 decisions it keeps with their reasoning.
`PLAN-V2.md` is the tasks that build it, tracked in #4. `device-agent-spec.md`
is the specification v1 was built to.

The agent needs MQTT 5. The fleet broker measured in M0, RabbitMQ 3.10.25, does
not accept it; RabbitMQ 4.1.8 and 4.3.5 do. `docs/spikes/m0-mqtt5.md` has the
measurements, and DESIGN-V2.md, "Broker constraints", what they mean for the
agent.

## Topics and messages

Every device has its own topics, and every agent its own status topic:

```
skuhus/<project>/<site>/<station>/<device>/rx               each frame read from the port
skuhus/<project>/<site>/<station>/<device>/status           the device's events
skuhus/<project>/<site>/<station>/<device>/tx               reserved; the agent subscribes from 2.1.0
skuhus/<project>/<site>/<station>/agent/<instance>/status   the agent's keepalive and offline message
```

`<project>`, `<site>` and `<station>` are the configuration's `identity`,
`<device>` is the device's `id`, and `<instance>` is `identity.instance`, which
defaults to the station. Each is a topic level, so each is `[a-z0-9-]+`. `agent`
is reserved, so no device can be configured into the agent's topics.

Every message is one JSON object, `schema` 2, that names the station, the
instance and the agent's version. Nothing is retained.

| Kind | Topic | QoS | Message expiry | Says |
|---|---|---|---|---|
| `rx` | `<device>/rx` | 1 | the device's `message_expiry` | the whole frame, separator excluded, as `raw_b64`, and as `text` when it is valid UTF-8; `seq` counts the device's frames from 1 |
| `event` | `<device>/status` | 1 | the device's | `port_opened`, `port_closed`, `port_lost`, `port_open_failed` or `bytes_discarded`, with the error class or the discard reason |
| `keepalive` | `agent/<instance>/status` | 0 | `gone_after_s` | every device's state and counters, every `status.keepalive_interval` and whenever the connection comes up |
| `offline` | `agent/<instance>/status` | 1 | none | `reason` `shutdown` when the agent stops cleanly, or `will`, published by the broker when the connection is lost |

A consumer treats an agent as gone after `gone_after_s` without a keepalive:
`status.missed_keepalives` intervals, 45 seconds by default. Each keepalive
carries the number, so consumers follow the agent's configuration. A consumer at
a station subscribes to:

```
skuhus/<project>/<site>/<station>/+/rx                every device's readings
skuhus/<project>/<site>/<station>/+/status            every device's events
skuhus/<project>/<site>/<station>/agent/+/status      every agent's keepalives and offline messages
```

A reading does not wait for the broker. One read while the connection is down
fails at once and is recorded in the log with its data. A device's events wait,
up to `status.event_buffer_size` of the most recent, and one older than the
device's `message_expiry` by then is dropped.

DESIGN-V2.md, "Message formats", has every field, with an example of each
message; the tests in `internal/wire` fail when the examples and the code
disagree.

## Build and test

Everything runs in Docker; nothing installs a toolchain on the host.

```
make              # the target list, with one line each
make build        # dist/skuhus-device-agent for this platform
make test         # go test -race across the agent's packages
make check        # gofmt, go vet, go mod tidy and the tests; what CI runs
make test-broker  # the consumer filters and topic permissions; needs make broker-up
make test-integration  # the agent's binary end to end; needs make broker-up
make cross        # all release targets: linux amd64/arm64/armv7/armv6, darwin amd64/arm64
make image        # the container image, tagged with the version in source
```

The tests need no device. `internal/device/serial` creates a pseudo-terminal
pair through `/dev/ptmx` and replays recorded byte streams through the real
serial library, so the read path, the framing and the disconnect handling are
exercised on every run.

`make test-integration` builds the agent with the race detector and runs the
binary as a station does: its device is a pseudo-terminal fed a recorded scanner
capture, and it reaches the development broker through a relay inside the test,
which can take the broker away mid-run. It checks every reading on the rx topic,
the device events, the keepalive's counters, the offline message on SIGTERM, the
will on SIGKILL, and the log's record of each reading the broker did not take.

The M0 broker spike checks the MQTT 5 properties the agent relies on, against
the local RabbitMQ or any other broker:

```
make broker-up
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03"
make spike-mqtt5 BROKER=host:1883 MQTT_USER=... MQTT_PASS=...
make broker-down
```

## Local broker

The fleet broker is RabbitMQ 3.10.25 and speaks no MQTT 5, so the agent cannot
be developed against it. `dev/rabbitmq/` runs a local RabbitMQ 4.3.5 that can:

```
make broker-up      # start it, wait for the MQTT listener, print the users
make broker-logs
make broker-down    # stop it, keep the data
make broker-reset   # discard the volume, so the next start rebuilds from definitions
```

MQTT is on 1883, AMQP on 5672 and the management UI on 15672, all bound to the
loopback interface. State lives in the `skuhus-dev-rabbitmq-data` volume and
survives `broker-down`.

Users, permissions and topic permissions come from
`dev/rabbitmq/definitions.json`, imported on every boot, so a broker rebuilt
from an empty volume comes back identical. **These credentials are development
fixtures in a file everyone can read.** They exist so a local broker needs no
setup steps; a station's real credentials come from `broker.credentials_file`
and never from a repository.

| User | Password | For |
|---|---|---|
| `admin` | `admin` | management UI |
| `station-pack-03` | `pack-03-dev` | an agent at station `pack-03` |
| `ingest` | `ingest-dev` | the consumer side, and tx senders |

Anonymous MQTT is refused, which is the fleet broker's current behaviour and the
reason this file sets it explicitly. `station-pack-03` is confined by topic
permission to `skuhus.acme.vasby.pack-03.*`: it cannot publish or subscribe
outside its own station, which is what device-agent-spec.md, section 8, asks
per-station credentials to buy. `ingest` reads every station's topics and
writes only device tx topics, `skuhus/<project>/<site>/<station>/<device>/tx`,
so it cannot pass anything off as a reading or a status. Adding a station means
adding a user and a topic permission to `definitions.json`.

The definitions are imported at boot, so a running broker takes a change at its
next restart, `make broker-down broker-up`, which keeps the volume. `make
test-broker` then checks the topic permissions above, along with the consumer
filters.

Point the spike at it to check the broker after a change:

```
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03"
```

## Watching what the broker sees

`dev/consumer` subscribes and prints. It is the other end of the wire during
development: the agent publishes, this prints, and the two together say whether
a reading left the building. It subscribes as `ingest`.

```
make consume                                          # every station, skuhus/#
make consume TOPIC='skuhus/acme/vasby/pack-03/+/rx'   # one station's readings
make consume FLAGS=--raw                              # payloads exactly as received
```

Each message prints its topic, QoS, retained flag and message expiry, a response
topic or correlation data when it carries one, then the payload with JSON
indented. An rx message gets one extra line decoding `raw_b64` back to bytes,
shown as text and hex, because a GS1-128 payload carries `0x1D` separators that
a quoted string hides.

A lost connection, or a DISCONNECT from the broker, ends the consumer with the
error printed. It does not reconnect: carrying on would hide the disconnect
someone is watching for.

## End to end on a workstation

The scanner is on the desk, the broker is in Docker, and the agent runs on the
host because Docker for Mac cannot see a USB serial device. Three terminals:

```
make broker-up                                  # RabbitMQ 4.3.5 on 127.0.0.1
make consume                                    # watch every topic
```

```
make cross                                      # dist/skuhus-device-agent-darwin-arm64
mkdir -p /tmp/skuhus-device-agent
SH_DEV_AGENT_MQTT_USERNAME=station-pack-03 \
SH_DEV_AGENT_MQTT_PASSWORD=pack-03-dev \
  ./dist/skuhus-device-agent-darwin-arm64 run --config dev/agent.local.yaml
```

`dev/agent.local.yaml` points at the local broker and at a Symbol 05e0:1701 on
a Mac; change `devices[0].path` to what `probe --list` reports on this host.
The credentials are the development fixtures from `dev/rabbitmq/definitions.json`
and are passed through the environment, so running this leaves no secret on
disk.

The consumer shows the agent's keepalive and the device's `port_opened` as soon
as the agent connects, a keepalive every 15 seconds after that, and an rx
message for each scan. Ctrl-C on the agent publishes the offline message with
reason `shutdown`.

## Running

```
skuhus-device-agent run --config /etc/skuhus-device-agent/config.yaml
```

Opens the configured devices, connects to the broker, and publishes what each
device reads until stopped. A device that is unplugged and a broker that is down
are both expected conditions: the agent keeps running, reopens the device and
reconnects with jittered backoff. Each failed attempt to open a device is a
`port_open_failed` event with its error class, and is counted in the keepalive.

SIGTERM and SIGINT stop it in this order: the keepalive stops; the devices
close, so nothing new arrives; what is already framed is published, for at most
5 seconds; the offline message goes out with reason `shutdown`; and only then
does the connection close. A reading still buffered after those 5 seconds is
recorded in the log as dropped, with its data, because a reading whose session
has ended is not worth delivering late. A clean stop exits 0, an error 1, and a
command line that is wrong 2.

Broker credentials come from `broker.credentials_file`:

```
username=station-pack-03
password=...
```

or from `SH_DEV_AGENT_MQTT_USERNAME` and `SH_DEV_AGENT_MQTT_PASSWORD`, which
override the file. There is no flag for them, because `ps` would expose them to
every user on the host.

### The log

There is one log. It goes to a file with size rotation (`logging.file`,
`logging.max_size_mb`, `logging.keep`), to stdout (`logging.stdout`, on by
default), to both, or to neither. Each record is a line of JSON with its
severity and the function, file and line that produced it.

Every reading gets a record of what became of it, whatever `logging.level`
says: `rx published`, `rx publish failed`, `rx dropped`, or `rx could not be
encoded` if its message could not be built, each with the message's `id`, the
device and `seq`. A reading the broker did not take carries its data,
as `data_hex`, and as `data_text` when it is valid UTF-8, because nothing else
holds it. With `logging.log_payloads` the data is on published readings and on
discard warnings too. Records at INFO and above are flushed to disk as they are
written, so the last ones before a power cut are on the disk.

The full rules are in DESIGN-V2.md, "Logging: one common log".

## Configuration

`config.sample.yaml` documents every setting. Install it at
`/etc/skuhus-device-agent/config.yaml` on Linux or
`/usr/local/etc/skuhus-device-agent/config.yaml` on macOS.

Precedence is CLI flags, then `SH_DEV_AGENT_*` environment variables, then the
config file, then defaults. `SH_DEV_AGENT_CONFIG` names the file. Every key
outside `devices` and `broker.connect_backoff` has a variable named after its
section and key, such as `SH_DEV_AGENT_BROKER_URL` for `broker.url`; devices
have none, because a list does not map onto flat variables. `run` and
`validate` take flags for the identity, the broker and the log:

```
--config --project --site --station --instance
--broker-url --broker-credentials-file --broker-ca-file --broker-insecure
--log-level --log-payloads
```

Unknown keys and unrecognised `SH_DEV_AGENT_*` variables are both fatal. So are
variables with an earlier release's prefix, `SKUHUS_AGENT_` or
`SH_DEV_SER_SCANNER_`: an upgraded station that still sets them would otherwise
run on its file's values in silence, so the error names the `SH_DEV_AGENT_`
variable that replaces each one. A key or variable that 2.0.0 removed, such as
`delivery.scan_ttl` or `logging.audit_file`, is rejected with what replaces it.
Broker credentials are never accepted as CLI arguments, because `ps` would
expose them to every user on the host.

```
skuhus-device-agent validate --config /etc/skuhus-device-agent/config.yaml
```

Every problem is reported in one pass, so a misconfigured station is fixed
without a restart per typo. Warnings are printed on stderr and do not affect the
exit code.

## Container

```
make image
```

Builds `skuhus-device-agent:<version>`, tagged and labelled with the version compiled
into the binary. The Makefile is the one place that reads that version; CI
asserts that the label and what the binary reports still agree.

Without make:

```
docker build \
  --build-arg COMMIT="$(git rev-parse HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t skuhus-device-agent:local .
```

The runtime image is Alpine, about 27MB, running as uid 65532. Alpine rather
than distroless or scratch on purpose: this agent fails in ways that live
outside the process - a device node owned by a group the container is not in, a
symlink that resolved to nothing, a udev rule that did not fire - and
diagnosing those means running `id` and `ls -l /dev` on the station where it is
happening.

```
docker exec skuhus-device-agent sh -c 'id; ls -l /dev/scanner'
docker run --rm --device /dev/ttyACM0 skuhus-device-agent:local probe --list
```

### Running it

```
docker run -d --name skuhus-device-agent --restart unless-stopped \
  --device "$(readlink -f /dev/serial/by-id/usb-Symbol_Bar_Code_Scanner-if00):/dev/scanner" \
  --group-add "$(stat -c %g "$(readlink -f /dev/serial/by-id/usb-Symbol_Bar_Code_Scanner-if00)")" \
  -v /etc/skuhus-device-agent:/etc/skuhus-device-agent:ro \
  -v skuhus-device-agent-log:/var/log/skuhus-device-agent \
  -e SH_DEV_AGENT_MQTT_USERNAME=station-pack-03 \
  -e SH_DEV_AGENT_MQTT_PASSWORD=... \
  skuhus-device-agent:local
```

Four things in that command are not decoration:

- **`--device src:/dev/scanner`.** The `/dev/serial/by-id` tree does not exist
  inside the container, so the stable path has to be resolved on the host and
  given a fixed name inside. The config then says `path: /dev/scanner`, which is
  stable for the same reason a by-id path is: it does not move when the kernel
  renames `ttyACM0`.
- **`--group-add`.** The device node is owned by a group - `dialout` on Debian -
  and uid 65532 is in no groups. Without this every open fails with
  `error_class` `permission_denied`, and the agent retries forever.
- **The config is mounted read-only**, at `/etc/skuhus-device-agent`. The image ships
  `config.sample.yaml` in that directory as a reference; the file the agent
  reads is `config.yaml`, which comes from the host.
- **The log file needs a writable mount** at `/var/log/skuhus-device-agent`,
  owned by 65532, when `logging.file` is set. A named volume gets this right; a
  host path needs `chown 65532:65532`.

With `logging.stdout` on, `docker logs skuhus-device-agent` shows the log as
well.

Credentials go in the environment or in a mounted credentials file. There is no
flag for them, and a URL carrying them is rejected at startup.

Docker Desktop on macOS cannot pass a USB device through to a container, so on
a Mac the agent runs on the host and the container is for Linux stations.

## Field diagnosis

`probe` is the tool to reach for first when a device is not delivering.

```
skuhus-device-agent probe --list
```

Enumerates the device nodes this host offers, and the `/dev/serial/by-id` and
`/dev/serial/by-path` symlinks that should be configured instead of the
kernel-assigned names.

```
skuhus-device-agent probe --device scanner-main
skuhus-device-agent probe --path /dev/serial/by-id/usb-Honeywell_1470g-if00 --separator '\r'
```

Opens one device, with the settings of `--device` in the config file or those
given as flags, and prints every frame as hex and as text, saying whether it is
valid UTF-8. Add `--json` to print the rx message that would be published, and
`--duration` to stop after a time. Diagnostic output goes to stdout and the
structured log to stderr, so the two can be redirected separately.

A separator that never appears in what the device sends, because it is
misconfigured or missing from the data, as from a scanner set up without a
suffix, means no reading is ever framed. The bytes are discarded instead, as
`inter_char_timeout` when the device goes quiet. `probe --log-payloads` puts
the discarded bytes on each discard's log line, which shows what the device
does send. From outside the station, the keepalive shows such a device with
`rx_bytes` and `discards.inter_char_timeout` rising while `rx_frames` stays
flat.

`probe` prints payload contents by design; `logging.log_payloads` does not apply
to it.

## Environment hazards on Linux

These bite before the agent is ever at fault, and the packaging that fixes them
is not written yet; PLAN-V2.md lists it outside #4.

- **ModemManager** opens `/dev/ttyACM*` on hotplug and sends AT commands at the
  scanner. It needs a udev rule setting `ENV{ID_MM_DEVICE_IGNORE}="1"`.
- **brltty** claims some USB-serial chipsets. Check for it when a device appears
  and then vanishes.
- The agent's user must be in the `dialout` group.

## Continuous integration

```
feature branch -> pull request -> merge to master -> build -> tag -> release
```

`ci.yml` guards pull requests: gofmt, `go vet`, a `go mod tidy` diff check, the
race-detector tests, the end-to-end test (`make broker-up` and `make
test-integration`: the agent's binary against RabbitMQ and a pseudo-terminal),
a cross-compile of every release target, and a container build that asserts the
image reports the version in source.

`release.yml` runs on every merge to master. It runs the same gates, builds all
six targets and the image, and only then, if the version in
`internal/version/version.go` is one that has not been tagged before, pushes the
image to GHCR and creates the tag and the release together.

So a release is made by editing that constant in a pull request. A merge that
did not change it is built and verified exactly the same way and then stops,
because that version is already out.

The tag is created by the release step rather than pushed separately, so a tag
and a release always appear together: a tag left behind by a failed publish
would make the next merge skip a release that never happened.

Every release carries, for each of linux amd64/arm64/armv7/armv6 and darwin
amd64/arm64:

- `skuhus-device-agent-<version>-<os>-<arch>.tar.gz`, holding the binary, the sample
  config, the README and the licence
- `skuhus-device-agent-<version>-<os>-<arch>`, the bare binary, for updating a station
  in place
- one `checksums.txt` covering all of them

Actions are pinned to commit SHAs; a tag can be moved to point at other code.

Not yet: golangci-lint, and deb and rpm packages. PLAN-V2.md lists both outside
#4.

## Releasing

The version lives in one place, the `version` constant in
`internal/version/version.go`. To release, raise it in a pull request. Merging
that pull request to master runs the release workflow, which tags `v<version>`
and publishes the release only after every build has passed, as "Continuous
integration" describes. Nothing is tagged by hand.

It is a constant rather than a linker flag so that `go build`, `go test`, an
IDE and the Makefile all report the same version, and no binary can claim a
version its source does not carry. The commit and build date are injected,
because source cannot know them.

## Github naming convention

Commit subjects and branch names start with the number of the issue they
belong to.

- Commit subject: `gh-<NNN> [<ACTION>] <COMMENT>`, for example
  `gh-8 [Fix] Reject environment variables with an earlier release's prefix`.
  The actions in use are `Add`, `Change` and `Fix`. The comment names the
  change, not the reader's reaction to it.
- Branch: `gh-<NNN>-<topic>`, for example `gh-8-rename`.
- Issue title: `[Feature]`, `[Task]` or `[Bug]`, then the subject. A task from
  PLAN-V2.md puts its number first: `[Task] T4: Give spike/ and dev/ their own
  modules`.

Dependabot names its own branches and commits.

## Layout

```
cmd/skuhus-device-agent/   main, flags, and the run, validate, probe and version commands
internal/config/           load, validate, defaults
internal/core/             runs the agent: each device's reader and publisher, events, keepalive, shutdown
internal/device/           Device interface
internal/device/serial/    CDC / RS-232 implementation, framing, PTY harness
internal/wire/             topics and the JSON of every message
internal/transport/mqtt/   autopaho wiring, publish semantics
internal/logging/          the common log and its file rotation
internal/logging/logtest/  the log captured and checked in tests
internal/version/          the release version, and the injected commit and date
test/integration/          the agent's binary end to end, behind the integration build tag
dev/rabbitmq/              local broker: compose, config, definitions
dev/consumer/              subscribes and prints, for watching the wire
dev/agent.local.yaml       agent config for a workstation and the local broker
Dockerfile                 build stage plus an Alpine runtime
.github/workflows/         ci and release
spike/brokerinfo/          what a broker is and which MQTT levels it answers
spike/mqtt5/               M0 broker property verification, not part of the agent
docs/scanners/             per-model scanner measurements
docs/spikes/               spike results
```

dev/ and spike/ are Go modules of their own, so `go vet ./...` and `go test
./...` in the repository root, and with them `make check` and CI, cover only
the agent. gofmt still checks every Go file.
