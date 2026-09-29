# Everything runs in Docker. Nothing here installs a toolchain on the host.
#
# Targets are grouped: build, check, image, and the development environment
# (a local broker, a consumer that prints what reaches it, and the M0 spike).

GO_IMAGE       ?= golang:1.25
RABBITMQ_IMAGE ?= rabbitmq:4.3.5-management
MOSQUITTO_IMAGE ?= eclipse-mosquitto:2.1.2-alpine
BIN            ?= skuhus-device-agent
IMAGE          ?= skuhus-device-agent

# The version is defined once, in Go source. This reads it; it is never injected.
VERSION := $(shell sed -n 's/^const version = "\(.*\)"/\1/p' internal/version/version.go)
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
	@echo "fmt        rewrite files with gofmt"
	@echo "clean      remove dist/ and coverage.out"
	@echo "version    print the version compiled into the binary"
	@echo
	@echo "broker-up      start the local RabbitMQ from dev/rabbitmq"
	@echo "broker-down    stop it, keeping its data"
	@echo "broker-reset   stop it and discard its volume"
	@echo "broker-logs    tail its log"
	@echo "consume        subscribe and print what reaches the broker"
	@echo
	@echo "spike-brokerinfo   what a broker is, and which MQTT levels it answers"
	@echo "spike-mqtt5        the M0 property spike; see docs/spikes/m0-mqtt5.md"
	@echo "spike-cluster-up   add a second broker node, for the retained check"
	@echo "spike-mosquitto-up a Mosquitto broker, which retains wills, for the spike"

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
		go -C dev run ./consumer --broker $(BROKER) --username ingest --password ingest-dev \
			--topic '$(TOPIC)' $(FLAGS)

.PHONY: spike-brokerinfo
spike-brokerinfo: caches network
	$(GO_NET) go -C spike run ./brokerinfo --mqtt $(BROKER) $(FLAGS)

.PHONY: spike-mqtt5
spike-mqtt5: caches network
	$(GO_NET) go -C spike run ./mqtt5 --broker $(BROKER) --username $(MQTT_USER) --password $(MQTT_PASS) $(FLAGS)
