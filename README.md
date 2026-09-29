# device-agent

Device agent that gives network access to devices physically attached to a host.
Phase 1 covers wired barcode scanners in USB-CDC mode.

It is a transport shim: it moves bytes and adds an envelope. It does not know
what a SKU is, it is not a ledger participant, and it does not do offline sync.
See `device-agent-spec.md` for the specification and `DESIGN.md` for the
decisions taken while implementing it.

## State

Milestones M1 and M2, plus the configuration and logging foundation. Working:

- serial device layer: open, framing, terminator handling, oversize and
  inter-character timeout discards, jittered reopen on disconnect
- scan envelope construction: the whole frame is the payload and `symbology` is
  always null
- the publish path: scans at QoS 1 with the message expiry interval set from
  `scan_ttl`, retained status with a last will, and a heartbeat every 15s
- every payload identified: `project`, `site`, `station` and `instance_id`, the
  first three being the topic segments that address the station
- configuration loading, merging and validation
- structured logging and the audit log
- `run`, `validate` and `probe` subcommands

Not built yet: the command channel and the feedback abstraction (M3), packaging
and CI (M5). See the end of `DESIGN.md`.

The M0 broker spike has been run, and it found that the deployed broker is
RabbitMQ 3.10.25, which does not speak MQTT 5 at all: the agent as specified
cannot connect to it. Against a local RabbitMQ 4.1.8, message expiry, retained
publish, request-response properties and will delivery all behave as M2 needs,
while two things do not - a will is delivered but never retained, and retained
messages do not cross cluster nodes, both landing on the retained status topic
in section 5.5. `docs/spikes/m0-mqtt5.md` has the measurements and the options.

## Build and test

Everything runs in Docker; nothing installs a toolchain on the host.

```
make              # the target list, with one line each
make build        # dist/skuhus-device-agent for this platform
make test         # go test -race across all packages
make check        # gofmt, go vet, go mod tidy and the tests; what CI runs
make cross        # all release targets: linux amd64/arm64/armv7/armv6, darwin amd64/arm64
make image        # the container image, tagged with the version in source
```

The broker spike runs against the local RabbitMQ, or against any other broker:

```
make broker-up
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03"
make spike-mqtt5 BROKER=host:1883 MQTT_USER=... MQTT_PASS=...
make broker-down
```

The tests need no scanner. `internal/device/serial` creates a pseudo-terminal
pair through `/dev/ptmx` and replays recorded byte streams through the real
serial library, so the read path, the framing and the disconnect handling are
exercised on every run.

## Local broker

The fleet broker is RabbitMQ 3.10.25 and speaks no MQTT 5, so M2 cannot be
developed against it. `dev/rabbitmq/` runs a local RabbitMQ 4.3.5 that can:

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
| `ingest` | `ingest-dev` | the consumer side |

Anonymous MQTT is refused, which is the fleet broker's current behaviour and the
reason this file sets it explicitly. `station-pack-03` is confined by topic
permission to `skuhus.acme.vasby.pack-03.*`: it cannot publish or subscribe
outside its own station, which is what section 8 asks per-station credentials to
buy. Adding a station means adding a user and a topic permission to
`definitions.json`.

Point the spike at it to check the broker after a change:

```
make spike-mqtt5 FLAGS="--prefix skuhus/acme/vasby/pack-03"
```

## Watching what the broker sees

`dev/consumer` subscribes and prints. It is the other end of the wire during
development: the agent publishes, this prints, and the two together say whether
a scan left the building.

```
make consume                                   # every station, skuhus/#
make consume TOPIC='skuhus/acme/vasby/pack-03/scan'
make consume FLAGS=--raw                       # payloads exactly as received
```

Each message prints its topic, QoS, retained flag and any MQTT 5 properties that
carry meaning here - message expiry, response topic, correlation data - then the
payload with JSON indented. A scan envelope gets one extra line decoding
`raw_b64` back to bytes, shown as text and hex, because a GS1-128 payload
carries `0x1D` separators that a quoted string hides.

Nothing publishes yet: there is no `run` subcommand until M2. To see the
consumer working before then, publish through the management API on 15672, or
run `make spike-mqtt5` against the same broker.

## Running

```
skuhus-device-agent run --config /etc/skuhus-device-agent/config.yaml
```

Opens the configured devices, connects to the broker, and publishes until
stopped. A device that is unplugged and a broker that is down are both expected
conditions: the agent keeps running, reports `device_present: false` in its
status and heartbeat, and reconnects with jittered backoff.

SIGTERM and SIGINT stop it in the order the delivery semantics require: the
devices stop first so nothing new arrives, what is already framed is published,
a retained `offline` status goes out, and only then does the connection close.
Anything still buffered after five seconds is dropped and recorded in the audit
log as `dropped`, because a scan whose session has ended is not worth delivering
late.

Broker credentials come from `broker.credentials_file`:

```
username=station-pack-03
password=...
```

or from `SH_DEV_AGENT_MQTT_USERNAME` and `SH_DEV_AGENT_MQTT_PASSWORD`, which
override the file. There is no flag for them, because `ps` would expose them to
every user on the host.

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

The runtime image is Alpine, about 26MB, running as uid 65532. Alpine rather
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
  -v skuhus-device-agent-audit:/var/log/skuhus-device-agent \
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
  and uid 65532 is in no groups. Without this the agent reports
  `error_class=permission_denied` and retries forever.
- **The config is mounted read-only**, at `/etc/skuhus-device-agent`. The image ships
  `config.sample.yaml` in that directory as a reference; the file the agent
  reads is `config.yaml`, which comes from the host.
- **The audit log needs a writable mount** at `/var/log/skuhus-device-agent`, owned by
  65532. A named volume gets this right; a host path needs
  `chown 65532:65532`.

Credentials go in the environment or in a mounted credentials file. There is no
flag for them, and a URL carrying them is rejected at startup.

Docker Desktop on macOS cannot pass a USB device through to a container, so on
a Mac the agent runs on the host and the container is for Linux stations.

## Continuous integration

```
feature branch -> pull request -> merge to master -> build -> tag -> release
```

`ci.yml` guards pull requests: gofmt, `go vet`, a `go mod tidy` diff check, the
race-detector tests, a cross-compile of every release target, and a container
build that asserts the image reports the version in source.

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

Not yet: golangci-lint, the Mosquitto integration job, and deb/rpm packaging
with GoReleaser, which needs the `packaging/` files that are still M5.

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

Scanning then prints an envelope in the consumer terminal within milliseconds.
Ctrl-C on the agent publishes the retained `offline` status, which the consumer
shows on its next start.

## Releasing

The version lives in one place, `internal/version/version.go`:

```go
const version = "0.1.0"
```

Edit it, then tag. It is a constant rather than a linker flag so that `go build`,
`go test`, an IDE and the Makefile all report the same version, and no binary
can claim a version its source does not carry. The commit and build date are
injected, because source cannot know them.

## Configuration

`config.sample.yaml` documents every setting. Install it at
`/etc/skuhus-device-agent/config.yaml` on Linux or
`/usr/local/etc/skuhus-device-agent/config.yaml` on macOS.

Precedence is CLI flags, then `SH_DEV_AGENT_*` environment variables, then the
config file, then defaults. Unknown keys and unrecognised `SH_DEV_AGENT_*`
variables are both fatal. So are variables with an earlier release's prefix,
`SKUHUS_AGENT_` or `SH_DEV_SER_SCANNER_`: an upgraded station that still sets
them would otherwise run on its file's values in silence, so the error names the
`SH_DEV_AGENT_` variable that replaces each one. Broker credentials are never
accepted as CLI arguments, because `ps` would expose them to every user on the
host.

```
skuhus-device-agent validate --config /etc/skuhus-device-agent/config.yaml
```

Every problem is reported in one pass, so a misconfigured station is fixed
without a restart per typo. Warnings are printed on stderr and do not affect the
exit code.

## Field diagnosis

`probe` is the tool to reach for first when a station is not scanning.

```
skuhus-device-agent probe --list
```

Enumerates the device nodes this host offers, and the `/dev/serial/by-id` and
`/dev/serial/by-path` symlinks that should be configured instead of the
kernel-assigned names.

```
skuhus-device-agent probe --device scanner-main
skuhus-device-agent probe --path /dev/serial/by-id/usb-Honeywell_1470g-if00 --terminator '\r'
```

Opens one device and prints every framed payload as hex and as text, saying
whether the payload is valid UTF-8. Add `--json` to print the exact envelope
that would be published. Diagnostic output goes to stdout and the structured log
to stderr, so the two can be redirected separately.

`probe` prints payload contents by design; `logging.log_payloads` does not apply
to it.

## Environment hazards on Linux

These bite before the agent is ever at fault, and the packaging that fixes them
is not written yet (M5).

- **ModemManager** opens `/dev/ttyACM*` on hotplug and sends AT commands at the
  scanner. It needs a udev rule setting `ENV{ID_MM_DEVICE_IGNORE}="1"`.
- **brltty** claims some USB-serial chipsets. Check for it when a device appears
  and then vanishes.
- The agent's user must be in the `dialout` group.

## Layout

```
cmd/skuhus-device-agent/   main, flags, subcommands
internal/config/           load, validate, defaults
internal/device/           Device interface
internal/device/serial/    CDC / RS-232 implementation, framing, PTY harness
internal/agent/            supervisor: devices, buffer, status, heartbeat
internal/transport/mqtt/   autopaho wiring, topics, publish semantics
internal/event/            envelope, encoding, ids
internal/logging/          slog setup, audit log
internal/version/          the release version, and the injected commit and date
dev/rabbitmq/              local broker: compose, config, definitions
dev/consumer/              subscribes and prints, for watching the wire
dev/agent.local.yaml       agent config for a workstation and the local broker
Dockerfile                 build stage plus a distroless runtime
.github/workflows/         ci and release
spike/brokerinfo/          what a broker is and which MQTT levels it answers
spike/mqtt5/               M0 broker property verification, not part of the agent
docs/scanners/             per-model scanner measurements
docs/spikes/               spike results
```
