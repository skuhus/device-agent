# Everything runs in Docker. Nothing here installs a toolchain on the host.
#
# Targets are grouped: build, check, image, and the development environment
# (a local broker, a consumer that prints what reaches it, and the M0 spike).

GO_IMAGE       ?= golang:1.25
RABBITMQ_IMAGE ?= rabbitmq:4.3.5-management
MOSQUITTO_IMAGE ?= eclipse-mosquitto:2.1.2-alpine
ASYNCAPI_IMAGE ?= asyncapi/cli:6.1.0
# The CLI image does not include the HTML template; the generator fetches it
# from npm at this version.
ASYNCAPI_HTML_TEMPLATE ?= @asyncapi/html-template@3.5.6
PANDOC_IMAGE ?= pandoc/core:3.7
BIN            ?= skuhus-device-agent
IMAGE          ?= skuhus-device-agent

# The version is defined once, in Go source. This reads it; it is never injected.
VERSION := $(shell sed -n 's/^const version = "\(.*\)"/\1/p' internal/version/version.go)
# The protocol's version, from the constant a test holds to asyncapi.yaml's
# info.version.
PROTOCOL_VERSION := $(shell sed -n 's/^const ProtocolVersion = "\(.*\)"/\1/p' internal/wire/messages.go)
COMMIT  := $(shell git rev-parse HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.commit=$(COMMIT) -X main.date=$(DATE)

# Release targets. armv7 is kept until the fleet is confirmed 64-bit.
PLATFORMS = linux/amd64 linux/arm64 linux/arm/7 linux/arm/6 darwin/amd64 darwin/arm64

# One network for everything in the development environment, so a container can
# always reach the broker by name.
NETWORK    := skuhus-dev
MOD_CACHE  := skuhus-device-agent-gomodcache
BUILD_CACHE := skuhus-device-agent-gobuildcache

# GO runs one command in the toolchain image with the module and build caches
# mounted and the source at /src. GO_NET is the same on the development network.
GO = docker run --rm \
	-v "$(CURDIR)":/src -v $(MOD_CACHE):/go/pkg/mod -v $(BUILD_CACHE):/root/.cache/go-build \
	-w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE)
GO_NET = docker run --rm --network $(NETWORK) \
	-v "$(CURDIR)":/src -v $(MOD_CACHE):/go/pkg/mod -v $(BUILD_CACHE):/root/.cache/go-build \
	-w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE)

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "$(BIN) $(VERSION)"
	@echo
	@echo "build      build dist/$(BIN) for this platform"
	@echo "cross      build every release target into dist/"
	@echo "image      build the container image, tagged $(IMAGE):$(VERSION)"
	@echo "check      gofmt, go vet, go mod tidy and the tests (what CI runs)"
	@echo "test       go test -race across the agent's packages"
	@echo "vet        go vet across the agent's packages"
	@echo "coverage   the tests, then each function's coverage"
	@echo "shell      a shell in the Go container, with the module and build caches"
	@echo "fmt        rewrite files with gofmt"
	@echo "clean      remove dist/ and coverage.out"
	@echo "version    print the version compiled into the binary"
	@echo
	@echo "protocol-check    validate protocol/asyncapi.yaml and the schemas it references"
	@echo "protocol-html     render it to dist/protocol/asyncapi-$(PROTOCOL_VERSION).html"
	@echo "protocol-notes    the release notes of protocol $(PROTOCOL_VERSION), from protocol/CHANGES.md"
	@echo "protocol-version  print the protocol version the agent speaks"
	@echo
	@echo "broker-up      start the local RabbitMQ from dev/rabbitmq"
	@echo "broker-down    stop it, keeping its data"
	@echo "broker-reset   stop it and discard its volume"
	@echo "broker-logs    tail its log"
	@echo "consume        subscribe and print what reaches the broker"
	@echo "send-tx        publish a tx to TX_TOPIC and print its results (FLAGS: --text, --file, --hex)"
	@echo "test-broker    publish the v2 messages through the broker, check each filter and the users' topic permissions"
	@echo "test-integration  run the agent against the broker and a pseudo-terminal, end to end"
	@echo
	@echo "spike-brokerinfo   what a broker is, and which MQTT levels it answers"
	@echo "spike-mqtt5        the M0 property spike; see docs/spikes/m0-mqtt5.md"
	@echo "spike-cluster-up   add a second broker node, for the retained check"
	@echo "spike-mosquitto-up a Mosquitto broker, which retains wills, for the spike"
	@echo "spike-serialbench  the serial printer bench, built to run on this host; see docs/printers/"

# --- build -----------------------------------------------------------------

.PHONY: caches
caches:
	@docker volume create $(MOD_CACHE) >/dev/null
	@docker volume create $(BUILD_CACHE) >/dev/null

.PHONY: build
build: caches
	$(GO) env CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BIN) ./cmd/skuhus-device-agent
	@echo "built dist/$(BIN) version=$(VERSION)"

# Cross-compiling every target on every check catches an arm-only breakage
# before a tag rather than after one.
.PHONY: cross
cross: caches
	@for platform in $(PLATFORMS); do \
		os=$${platform%%/*}; rest=$${platform#*/}; arch=$${rest%%/*}; arm=$${rest#*/}; \
		[ "$$arm" = "$$arch" ] && arm=""; \
		out=dist/$(BIN)-$$os-$$arch$${arm:+v$$arm}; \
		echo "building $$out"; \
		$(GO) env CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch GOARM=$$arm \
			go build -trimpath -ldflags "$(LDFLAGS)" -o $$out ./cmd/skuhus-device-agent || exit 1; \
	done
	@ls -la dist/

# The version label is applied here rather than inside the Dockerfile, because
# this file is the one place that reads the version out of the source.
.PHONY: image
image:
	docker build \
		--build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(DATE) \
		--label org.opencontainers.image.version=$(VERSION) \
		--label org.opencontainers.image.revision=$(COMMIT) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .
	@echo "built $(IMAGE):$(VERSION)"

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	rm -rf dist coverage.out

# --- check -----------------------------------------------------------------

.PHONY: check
check: fmt-check vet tidy-check test

.PHONY: fmt
fmt: caches
	$(GO) gofmt -w .

.PHONY: fmt-check
fmt-check: caches
	@out=$$($(GO) gofmt -l .); \
	if [ -n "$$out" ]; then echo "not gofmt clean:"; echo "$$out"; exit 1; fi
	@echo "gofmt clean"

.PHONY: vet
vet: caches
	$(GO) go vet ./...

.PHONY: tidy-check
tidy-check: caches
	$(GO) sh -c 'cp go.mod go.mod.bak && cp go.sum go.sum.bak && \
		go mod tidy && \
		diff -u go.mod.bak go.mod && diff -u go.sum.bak go.sum; \
		status=$$?; mv go.mod.bak go.mod; mv go.sum.bak go.sum; exit $$status'
	@echo "go mod tidy is clean"

# The PTY harness needs /dev/ptmx, which the container provides. The race
# detector needs cgo, so this is the one place CGO is enabled; released binaries
# are built with CGO_ENABLED=0 and are fully static.
.PHONY: test
test: caches
	$(GO) env CGO_ENABLED=1 go test -race -coverprofile=coverage.out -covermode=atomic ./...

.PHONY: coverage
coverage: test
	$(GO) go tool cover -func=coverage.out

.PHONY: shell
shell: caches
	docker run --rm -it \
		-v "$(CURDIR)":/src -v $(MOD_CACHE):/go/pkg/mod -v $(BUILD_CACHE):/root/.cache/go-build \
		-w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE) bash

# --- protocol --------------------------------------------------------------

# CI=true turns off the CLI's usage tracking, which otherwise reports every run
# (lib/apps/cli/internal/base.js in asyncapi/cli 6.1.0).
ASYNCAPI = docker run --rm -e CI=true -v "$(CURDIR)/protocol":/protocol:ro

.PHONY: protocol-check
protocol-check:
	$(ASYNCAPI) $(ASYNCAPI_IMAGE) validate /protocol/asyncapi.yaml

# The image runs as its own user, which cannot write to a directory mounted
# from a Linux host, so the page is written inside the container and streamed
# out. PUPPETEER_SKIP_DOWNLOAD skips the browser the template installs for PDF
# output, which is not made here.
.PHONY: protocol-html
protocol-html:
	@mkdir -p dist/protocol
	$(ASYNCAPI) -e PUPPETEER_SKIP_DOWNLOAD=true --entrypoint sh $(ASYNCAPI_IMAGE) -c ' \
		asyncapi generate fromTemplate /protocol/asyncapi.yaml $(ASYNCAPI_HTML_TEMPLATE) \
			--output /tmp/html --force-write --no-interactive -p singleFile=true -p outFilename=asyncapi.html >&2 && \
		cat /tmp/html/asyncapi.html' > dist/protocol/asyncapi-$(PROTOCOL_VERSION).html.part
	@mv dist/protocol/asyncapi-$(PROTOCOL_VERSION).html.part dist/protocol/asyncapi-$(PROTOCOL_VERSION).html
	@ls -l dist/protocol/asyncapi-$(PROTOCOL_VERSION).html

# The version's section of CHANGES.md, with each paragraph and list item on one
# line: GitHub shows every newline in release notes as a line break, and
# CHANGES.md is wrapped. A version with no section has no notes, and fails.
.PHONY: protocol-notes
protocol-notes:
	@mkdir -p dist
	@awk -v heading="## $(PROTOCOL_VERSION)" '/^## / { inside = ($$0 == heading); next } inside { print }' \
		protocol/CHANGES.md > dist/protocol-notes.md.part
	@grep -q '[^[:space:]]' dist/protocol-notes.md.part || \
		{ echo 'protocol/CHANGES.md has no section "## $(PROTOCOL_VERSION)"' >&2; rm dist/protocol-notes.md.part; exit 1; }
	docker run --rm -i $(PANDOC_IMAGE) --from gfm --to gfm --wrap=none < dist/protocol-notes.md.part > dist/protocol-notes.md
	@rm dist/protocol-notes.md.part
	@cat dist/protocol-notes.md

.PHONY: protocol-version
protocol-version:
	@echo $(PROTOCOL_VERSION)

# --- development environment ----------------------------------------------

COMPOSE = RABBITMQ_IMAGE=$(RABBITMQ_IMAGE) docker compose -f dev/rabbitmq/compose.yaml

.PHONY: network
network:
	@docker network inspect $(NETWORK) >/dev/null 2>&1 || docker network create $(NETWORK) >/dev/null

.PHONY: broker-up
broker-up: network
	$(COMPOSE) up -d --wait
	@docker exec skuhus-dev-rabbitmq rabbitmqctl -q list_users

.PHONY: broker-down
broker-down:
	$(COMPOSE) down
	@docker rm -f skuhus-dev-rabbitmq-2 skuhus-dev-mosquitto >/dev/null 2>&1 || true

.PHONY: broker-reset
broker-reset:
	$(COMPOSE) down -v
	@docker rm -f skuhus-dev-rabbitmq-2 skuhus-dev-mosquitto >/dev/null 2>&1 || true

.PHONY: broker-logs
broker-logs:
	$(COMPOSE) logs --tail 50 rabbitmq

# Section 5.1 asks specifically about retained messages across cluster nodes,
# which one node cannot answer. This joins a second node to the first, taking
# the first node's Erlang cookie rather than imposing one, so the running broker
# and its volume are left alone.
.PHONY: spike-cluster-up
spike-cluster-up: broker-up
	@docker rm -f skuhus-dev-rabbitmq-2 >/dev/null 2>&1 || true
	@docker run -d --name skuhus-dev-rabbitmq-2 --hostname rmq2 --network $(NETWORK) \
		-e RABBITMQ_ERLANG_COOKIE="$$(docker exec skuhus-dev-rabbitmq cat /var/lib/rabbitmq/.erlang.cookie)" \
		-e RABBITMQ_NODENAME=rabbit@rmq2 \
		$(RABBITMQ_IMAGE) \
		sh -c 'echo "[rabbitmq_management,rabbitmq_mqtt]." > /etc/rabbitmq/enabled_plugins && exec docker-entrypoint.sh rabbitmq-server' >/dev/null
	@until docker exec skuhus-dev-rabbitmq-2 rabbitmq-diagnostics -q check_running >/dev/null 2>&1; do sleep 3; done
	@docker exec skuhus-dev-rabbitmq-2 sh -c 'rabbitmqctl -q stop_app && rabbitmqctl -q reset && rabbitmqctl -q join_cluster rabbit@skuhus-dev-rabbitmq && rabbitmqctl -q start_app'
	@docker exec skuhus-dev-rabbitmq rabbitmqctl -q cluster_status | grep -A3 "Running Nodes"

# RabbitMQ 4.1.8 and 4.3.5 do not retain a will (docs/spikes/m0-mqtt5.md), so
# the spike clearing a stored will can only be exercised against a broker that
# does. Mosquitto does, and here accepts any credentials, so the spike targets
# work against it unchanged.
.PHONY: spike-mosquitto-up
spike-mosquitto-up: network
	@docker rm -f skuhus-dev-mosquitto >/dev/null 2>&1 || true
	@docker run -d --name skuhus-dev-mosquitto --network $(NETWORK) $(MOSQUITTO_IMAGE) \
		sh -c 'printf "listener 1883\nallow_anonymous true\n" > /mosquitto/config/mosquitto.conf && exec mosquitto -c /mosquitto/config/mosquitto.conf' >/dev/null
	@for attempt in $$(seq 1 30); do \
		docker logs skuhus-dev-mosquitto 2>&1 | grep -q " running" && exit 0; \
		[ "$$(docker inspect -f '{{.State.Running}}' skuhus-dev-mosquitto)" = true ] || break; \
		sleep 1; \
	done; \
	echo "mosquitto did not start:"; docker logs skuhus-dev-mosquitto 2>&1 | tail -5; exit 1
	@echo "mosquitto at skuhus-dev-mosquitto:1883; use BROKER=skuhus-dev-mosquitto:1883"

# The broker, the credentials and the topic to watch. Override any of them to
# point these at something else.
#
# Not named USER: make inherits the environment, and every shell exports USER,
# so a variable by that name silently becomes the login name.
BROKER    ?= skuhus-dev-rabbitmq:1883
MQTT_USER ?= station-pack-03
MQTT_PASS ?= pack-03-dev
# The development user that stands in for consumers and tx senders.
INGEST_USER ?= ingest
INGEST_PASS ?= ingest-dev
# Escaped because make would otherwise read the hash as a comment, leaving a
# subscription to "skuhus/" that receives nothing.
TOPIC     ?= skuhus/\#
FLAGS     ?=

# dev/ and spike/ are Go modules of their own, so the agent's vet and tests do
# not compile them; -C runs each command inside its module.
.PHONY: consume
consume: caches network
	docker run --rm -it --network $(NETWORK) \
		-v "$(CURDIR)":/src -v $(MOD_CACHE):/go/pkg/mod -v $(BUILD_CACHE):/root/.cache/go-build \
		-w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE) \
		go -C dev run ./consumer --broker $(BROKER) --username $(INGEST_USER) --password $(INGEST_PASS) \
			--topic '$(TOPIC)' $(FLAGS)

# A tx, sent as the ingest user. The binary is built first and run from /src, so
# that --file takes a path from the repository root.
TX_TOPIC ?= skuhus/acme/vasby/pack-03/scanner-main/tx

.PHONY: send-tx
send-tx: caches network
	$(GO_NET) sh -c 'go -C dev build -o /tmp/sendtx ./sendtx && /tmp/sendtx --broker $(BROKER) \
		--username $(INGEST_USER) --password $(INGEST_PASS) --topic "$(TX_TOPIC)" $(FLAGS)'

# The consumer filters in internal/wire, checked against a real broker and its
# topic permissions. Not part of check, which needs no broker: run broker-up
# first.
.PHONY: test-broker
test-broker: caches network
	$(GO_NET) env TEST_BROKER=$(BROKER) TEST_MQTT_USER=$(MQTT_USER) TEST_MQTT_PASS=$(MQTT_PASS) \
		TEST_INGEST_USER=$(INGEST_USER) TEST_INGEST_PASS=$(INGEST_PASS) \
		go test -count=1 -run Broker -v ./internal/wire/

# The agent's binary against the broker and a pseudo-terminal device, end to
# end (PLAN-V2.md, T10). The tag keeps it out of make check, which needs no
# broker, so it is vetted here with the tag.
.PHONY: test-integration
test-integration: caches network
	$(GO_NET) sh -c 'go vet -tags integration ./test/integration/ && \
		TEST_BROKER=$(BROKER) TEST_MQTT_USER=$(MQTT_USER) TEST_MQTT_PASS=$(MQTT_PASS) \
		TEST_INGEST_USER=$(INGEST_USER) TEST_INGEST_PASS=$(INGEST_PASS) \
		go test -count=1 -race -tags integration -v ./test/integration/'

.PHONY: spike-brokerinfo
spike-brokerinfo: caches network
	$(GO_NET) go -C spike run ./brokerinfo --mqtt $(BROKER) $(FLAGS)

.PHONY: spike-mqtt5
spike-mqtt5: caches network
	$(GO_NET) go -C spike run ./mqtt5 --broker $(BROKER) --username $(MQTT_USER) --password $(MQTT_PASS) $(FLAGS)

# The serial bench runs on the host, next to the device: Docker Desktop on macOS
# cannot pass a USB serial device through to a container. It is built here and
# run from dist/.
HOST_OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH := $(shell uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')

.PHONY: spike-serialbench
spike-serialbench: caches
	$(GO) env CGO_ENABLED=0 GOOS=$(HOST_OS) GOARCH=$(HOST_ARCH) \
		go -C spike build -trimpath -o ../dist/serialbench-$(HOST_OS)-$(HOST_ARCH) ./serialbench
	@echo "built dist/serialbench-$(HOST_OS)-$(HOST_ARCH); run it on this host"
