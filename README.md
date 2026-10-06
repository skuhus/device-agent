# device-agent

Device agent that gives network access to devices physically attached to a host.
It reads bytes from a serial port, USB-CDC or RS-232, and publishes each frame
to an MQTT broker, and it writes to the port the bytes that senders publish for
it.

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
skuhus/<project>/<site>/<station>/<device>/status           the device's events and tx results
skuhus/<project>/<site>/<station>/<device>/tx               bytes for the port, from senders
skuhus/<project>/<site>/<station>/agent/<instance>/status   the agent's keepalive and offline message
```

A tx can also go to every device in a broadcast group, on the group's topic
("Writing to a device"):

```
skuhus/<project>/group/<group>/tx                           every device in the project's group
skuhus/<project>/<site>/group/<group>/tx                    every device in the site's group
skuhus/<project>/<site>/<station>/group/<group>/tx          every device in the station's group
```

`<project>`, `<site>` and `<station>` are the configuration's `identity`,
`<device>` is the device's `id`, `<group>` a group in its `broadcast_groups`,
and `<instance>` is `identity.instance`, which defaults to the station. Each is a
topic level, so each is `[a-z0-9-]+`. `agent` is reserved, so no device can be
configured into the agent's topics, and `group` is reserved as a station, whose
device topics would be the site's group topics.

Every message is one JSON object, `schema` 2, that names the station, the
instance and the agent's version. Nothing is retained.

| Kind | Topic | QoS | Message expiry | Says |
|---|---|---|---|---|
| `rx` | `<device>/rx` | 1 | the device's `message_expiry` | the whole frame, separator excluded, as `raw_b64`, and as `text` when it is valid UTF-8; `seq` counts the device's frames from 1 |
| `event` | `<device>/status` | 1 | the device's | `port_opened`, `port_closed`, `port_lost`, `port_open_failed` or `bytes_discarded`, with the error class or the discard reason |
| `tx_result` | `<device>/status` | 1 | the device's | what became of a tx: `accepted`, then `written` or `failed`, or `rejected` for a resend of a tx still in hand; a code, and the bytes written |
| `keepalive` | `agent/<instance>/status` | 0 | `gone_after_s` | every device's state and counters, and every topic that reaches its tx with the broker's answer to the subscription, every `status.keepalive_interval` and whenever the connection comes up |
| `offline` | `agent/<instance>/status` | 1 | none | `reason` `shutdown` when the agent stops cleanly, or `will`, published by the broker when the connection is lost |

A consumer treats an agent as gone after `gone_after_s` without a keepalive:
`status.missed_keepalives` intervals, 45 seconds by default. Each keepalive
carries the number, so consumers follow the agent's configuration. A consumer at
a station subscribes to:

```
skuhus/<project>/<site>/<station>/+/rx                every device's readings
skuhus/<project>/<site>/<station>/+/status            every device's events and tx results
skuhus/<project>/<site>/<station>/agent/+/status      every agent's keepalives and offline messages
```

A reading does not wait for the broker. One read while the connection is down
fails at once and is recorded in the log with its data. A device's events wait,
up to `status.event_buffer_size` of the most recent, and one older than the
device's `message_expiry` by then is dropped.

The protocol is in [protocol/](protocol/), versioned on its own:
`asyncapi.yaml` describes every topic and message, with an example of each,
`messages.schema.json` defines every field in JSON Schema, and `CHANGES.md`
lists the versions and the agent versions that speak each. The tests in
`internal/wire` fail when the code and protocol/ disagree. Each protocol
version is released as `protocol-v<version>`, with those files and a rendered
page, and from protocol 2.2.0 the keepalive carries `protocol_version`.

## Writing to a device

A sender publishes a tx to the device's tx topic, at QoS 1 and with a message
expiry:

```json
{"schema": 2, "id": "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b", "sender": "label-service", "raw_b64": "XlhBXkZEU0tVLTEwNDJeRlNeWFo="}
```

`id` is a UUID, and every field is required. The agent answers on the device's
status topic: `accepted` at once, then `written` once the operating system has
taken every byte, or `failed` with a code. A tx that cannot be read fails at
once; one whose id is still queued or being written is `rejected` with where
that one stands; and one whose id was written recently gets `already_written`
and is not written again. The agent remembers the last `tx_remembered_ids` ids
written to each device, 1024 by default, until it restarts. A device's tx are written one at a time, whole, in
the order they arrived, while reading carries on.

`written` means the operating system took the bytes, not that the device has
them. On the bench printer, an Epson TM-T20III behind a USB to RS-232 adapter
at 9600 baud, the adapter's driver took a job 16 KB at a time, so `written`
came up to about 17 s before the printer had the last bytes
(docs/printers/epson-tm-t20iii.md).

A tx that finds the port closed asks for it to be opened, up to
`tx_open_attempts` times, `tx_open_interval` apart (3 and 1 s by default), and
then fails as `port_unavailable` with the reason. Once writing has started
nothing is retried, since a retry could print a job twice; `write_failed` says
how many bytes were written. No attempt is made once the tx's message expiry
has passed.

A tx the agent stops before writing fails as `agent_stopping`, with the bytes
written: one being written goes on for `delivery.drain_timeout`, 5 s by default,
first and then stops after the chunk
in hand, which on the bench adapter took up to 16 s more; one still queued is
not started. A tx published while the agent is disconnected is lost and gets no
result, and a sender treats a tx that got no result as not written.
DESIGN-V2.md, "The tx contract", has the rest.

A device can be in broadcast groups, under `broadcast_groups` in its entry, at
the project, the site or the station:

```yaml
broadcast_groups: { project: [], site: [scales], station: [] }
```

A tx on a group's topic is taken by every device in the group whose agent is
connected, as if it had been sent to each on its own topic: each writes it and answers on its own
status topic, all under the tx's id, and a resend is answered by each for
itself. The agent subscribes to each group's topic once, and each keepalive
lists every topic that reaches a device, as subscribed, with the broker's
answer. `validate` shows each group's topic.

The station's broker user must be allowed to read its project's and its site's
group topics before a station joins a group there. RabbitMQ 4.3.5 closes the
connection of a client whose subscription it refuses, and the agent then
reconnects after `broker.reconnect_interval`, 1 s by default, and is refused
again, until the permission is granted or the group removed (#38).
DESIGN-V2.md, "Broadcast groups", has the rest.

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
permission to `skuhus.acme.vasby.pack-03.*`: it cannot publish outside its own
station, which is what device-agent-spec.md, section 8, asks per-station
credentials to buy, and outside it may read only its project's and its site's
broadcast group tx topics. `ingest` reads every station's topics and writes only
tx topics, a device's or a broadcast group's, so it cannot pass anything off as
a reading or a status. Adding a station means adding a user and a topic
permission to `definitions.json`.

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

`dev/sendtx` is the sending side: it publishes a tx as `ingest`, then prints
the results the agent publishes for it.

```
make send-tx TX_TOPIC=skuhus/acme/vasby/pack-03/printer-1/tx FLAGS="--file dist/job.bin"
make send-tx TX_TOPIC=skuhus/acme/vasby/pack-03/printer-1/tx FLAGS="--hex 1b40"
make send-tx TX_TOPIC=skuhus/acme/vasby/group/scales/tx FLAGS="--hex 05"
```

Sent to a broadcast group, it prints every device's results, with the station
and the device, until `--wait` ends, 15 s by default. `--file` takes a path from
the repository root. `--id` sends a chosen id, to see a resend rejected while
the first is still being written.

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

Opens the configured devices, connects to the broker, publishes what each device
reads, and writes the tx each is sent, until stopped. A device that is unplugged
and a broker that is down are both expected conditions: the agent keeps running.
It reopens the device after `reopen_interval`, 100 ms, and `reopen_backoff`
makes that wait grow, up to 30 s by default. It tries the broker at once at
start, and then every second, `broker.reconnect_interval`, which it also waits
after a lost connection; `broker.reconnect_backoff` makes that wait grow
instead. Each failed attempt to open a device is a
`port_open_failed` event with its error class, and is counted in the keepalive.

SIGTERM and SIGINT stop it in this order: no tx is started any more, and one
being written gets `delivery.drain_timeout`, 5 seconds by default, and then
stops after the chunk in hand; the keepalive stops; the devices close, so
nothing new arrives; what is already framed is published, for at most the same
time again; the offline message goes out with reason `shutdown`; and only then
does the connection close. A reading still buffered after that is recorded in
the log as dropped, with its data,
because a reading whose session has ended is not worth delivering late. A clean
stop exits 0. An unknown command, a flag that does not exist or does not parse,
and a positional argument exit 2, and any other error 1.

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
outside `devices` and `broker.reconnect_backoff` has a variable named after its
section and key, such as `SH_DEV_AGENT_BROKER_URL` for `broker.url`; those two
have none, because a list or a mapping does not map onto flat variables. `run`
and `validate` take flags for the identity, the broker and the log:

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

It states its result on the first line. A usable configuration gives `OK`, on
stdout, followed by every setting, and exit status 0:

```
OK: /etc/skuhus-device-agent/config.yaml is valid
```

Any other gives `ERROR`, on stderr, with every problem found, one a line, and
exit status 1, so a misconfigured station is fixed without a restart per typo:

```
ERROR: /etc/skuhus-device-agent/config.yaml is not valid: 2 problems
  - identity.station "pack_03" must match [a-z0-9-]+; it is used verbatim as an MQTT topic segment
  - devices.scanner-main.baud must be positive, got 0
```

Warnings are printed on stderr, before the result, and do not change it:
`OK: ... is valid, with 1 warning above`.

## Upgrading

What a station or a consumer has to change for a release is listed here, and
each release's notes link to this section.

### From 0.3.0 to 2.0.0

0.3.0 was skuhus-device-serial-scanner, and 2.0.0 does not run its setup
unchanged.

On a station:
- The binary is `skuhus-device-agent`, its configuration
  `/etc/skuhus-device-agent/config.yaml` (`/usr/local/etc/skuhus-device-agent/`
  on macOS), its log directory `/var/log/skuhus-device-agent`, and its image
  `ghcr.io/skuhus/skuhus-device-agent`, running as `skuhus-device-agent`, uid
  65532 as before. 0.3.0 used `skuhus-device-serial-scanner` for each.
- Environment variables start with `SH_DEV_AGENT_` instead of
  `SH_DEV_SER_SCANNER_`. A variable with the old prefix is refused, with the
  name that replaces it.
- The configuration file changed. Start from `config.sample.yaml`; `validate`
  names each key 0.3.0 had and 2.0.0 does not, with what replaces it:

| 0.3.0 | 2.0.0 |
|---|---|
| `devices[].terminator` | `devices[].separator` |
| `devices[].assert_config` | none: the agent does not configure devices |
| `delivery.scan_ttl` | `devices[].message_expiry`, per device |
| `broker.connect_backoff` | `broker.reconnect_interval`, and `broker.reconnect_backoff` to make the wait grow |
| `logging.audit_file`, `audit_max_size_mb`, `audit_keep` | `logging.file`, `max_size_mb`, `keep`: one log for everything |

For the services that consume the messages:
- 0.3.0 published on `skuhus/<project>/<site>/<station>/scan`, `/status` and
  `/heartbeat`. 2.0.0 publishes on each device's own topics and on the agent's
  status topic, as "Topics and messages" lists: a reading is `rx`, on
  `<device>/rx`.
- Messages are `schema` 2, where 0.3.0's were 1, and nothing is retained: 0.3.0
  retained its status and its will. A consumer learns that an agent is there
  from its keepalives, and that it is gone after `gone_after_s` without one.
- 2.0.0 writes to devices: a sender publishes a tx on `<device>/tx` ("Writing
  to a device").

## Container

Each release publishes the image as `ghcr.io/skuhus/skuhus-device-agent`, tagged
with its version and with `latest`:

```
docker pull ghcr.io/skuhus/skuhus-device-agent:<version>
```

To build it from a checkout instead:

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

The image declares no VOLUME: Docker would create an anonymous volume on every
run that did not mount over it, and those accumulate unnoticed. The two paths
that want mounting are in the command below.

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

With the published image, `ghcr.io/skuhus/skuhus-device-agent:<version>` takes
the place of `skuhus-device-agent:local`.

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
valid UTF-8. With `--path`, `--separator` is required, as it is in the config
file, and a flag left out takes the config file's default; with `--device`, a
flag for a device setting is refused rather than ignored. Add `--json` to print the rx message that would be published, and
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

An agent whose log repeats `broker connected`, `subscription refused; no tx will
arrive on this topic` and `broker connection lost, reconnecting`, once every
`broker.reconnect_interval`, subscribes to a topic its broker user may not read,
usually a broadcast group's; the refused record names the topic. RabbitMQ 4.3.5
closes the connection over it, and the agent tries again after the interval.
Grant the read permission, or take the group out of `broadcast_groups`
("Writing to a device").

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
internal/config/           the file, the environment variables, validation and defaults
internal/backoff/          the wait between attempts, for a device's reopen and the broker's reconnect
internal/core/             runs the agent: each device's frames, status messages, tx, keepalive, shutdown
internal/device/           Device interface
internal/device/serial/    CDC / RS-232 implementation, framing, PTY harness
internal/wire/             topics and the JSON of every message, held to protocol/ by its tests
protocol/                  the MQTT protocol: AsyncAPI document, JSON Schema, versions
internal/transport/mqtt/   the broker connection: autopaho wiring, publishing, subscribing
internal/logging/          the common log and its file rotation
internal/logging/logtest/  the log captured and checked in tests
internal/version/          the release version, and the injected commit and date
test/integration/          the agent's binary end to end, behind the integration build tag
dev/rabbitmq/              local broker: compose, config, definitions
dev/consumer/              subscribes and prints, for watching the wire
dev/sendtx/                sends a tx and prints its results
dev/internal/mqttconn/     connects and subscribes the dev tools
dev/agent.local.yaml       agent config for a workstation and the local broker
Dockerfile                 build stage plus an Alpine runtime
.github/workflows/         ci, the agent's release and the protocol's
spike/brokerinfo/          what a broker is and which MQTT levels it answers
spike/mqtt5/               M0 broker property verification, not part of the agent
spike/serialbench/         a serial printer's line settings and write timing, run on the host
docs/scanners/             per-model scanner measurements
docs/printers/             per-model printer measurements
docs/spikes/               spike results
docs/rabbitmq/             an example RabbitMQ broker set up for a deployment
```

dev/ and spike/ are Go modules of their own, so `go vet ./...` and `go test
./...` in the repository root, and with them `make check` and CI, cover only
the agent. gofmt still checks every Go file.
