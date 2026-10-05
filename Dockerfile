# syntax=docker/dockerfile:1

# Build on the machine's own architecture and cross-compile from there: the Go
# toolchain does that far faster than emulating the target.
FROM --platform=$BUILDPLATFORM golang:1.25 AS build

WORKDIR /src

# Module files first. Dependencies change far less often than source, so this
# keeps their download out of the layer every code edit invalidates.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The commit and the build date are the only identity the source cannot know.
# The version is a constant in internal/version and is compiled in.
ARG COMMIT=none
ARG BUILD_DATE=unknown

# Set by buildx per target platform, and defaulted so a plain "docker build"
# works. TARGETVARIANT is "v6" or "v7" for 32-bit arm and empty otherwise, which
# is exactly GOARM with the "v" removed.
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG TARGETVARIANT=

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath \
    -ldflags "-s -w -X main.commit=$COMMIT -X main.date=$BUILD_DATE" \
    -o /out/skuhus-device-agent ./cmd/skuhus-device-agent

# Alpine rather than a distroless or scratch base, deliberately. This agent
# fails in ways that live outside the process: a device node owned by a group
# the container is not in, a symlink that resolved to nothing, a udev rule that
# did not fire. Diagnosing those needs "id", "ls -l /dev" and a shell to run
# them in, on the station where it is happening. An image that cannot be
# inspected trades a few megabytes for an engineer's afternoon.
FROM alpine:3.24

# ca-certificates for TLS to the broker. tzdata because a log read at a station
# is read in local time even though every timestamp on the wire is UTC.
RUN apk add --no-cache ca-certificates tzdata

# A fixed uid and gid, so a host directory mounted for the log file can be
# chowned to a number that does not change between builds.
RUN addgroup -S -g 65532 skuhus-device-agent \
 && adduser -S -D -H -u 65532 -G skuhus-device-agent \
      -s /sbin/nologin skuhus-device-agent \
 && mkdir -p /etc/skuhus-device-agent /var/log/skuhus-device-agent \
 && chown skuhus-device-agent:skuhus-device-agent /var/log/skuhus-device-agent

COPY --from=build /out/skuhus-device-agent /usr/local/bin/skuhus-device-agent
COPY config.sample.yaml /etc/skuhus-device-agent/config.sample.yaml

# Facts that do not vary with the build. The version and the commit are passed
# as --label at build time by the Makefile, which is the one place that reads
# the version; putting them here would mean a second reader that can drift.
LABEL org.opencontainers.image.title="skuhus-device-agent" \
      org.opencontainers.image.description="Device agent that moves bytes between serial devices and MQTT" \
      org.opencontainers.image.source="https://github.com/skuhus/device-agent" \
      org.opencontainers.image.licenses="MIT"

# No VOLUME instruction: it would create an anonymous volume on every run that
# did not mount over it, and those accumulate unnoticed. Two paths want mounting
# and the README says so - /etc/skuhus-device-agent read-only for the config, and
# /var/log/skuhus-device-agent writable for the log file.

USER skuhus-device-agent
ENTRYPOINT ["skuhus-device-agent"]
CMD ["run", "--config", "/etc/skuhus-device-agent/config.yaml"]
