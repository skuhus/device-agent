package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/skuhus/device-agent/internal/logging/logtest"
)

func TestExpirySecondsRoundsUp(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want uint32
	}{
		{30 * time.Second, 30},
		{2500 * time.Millisecond, 3}, // Rounding down would expire a scan early.
		{time.Millisecond, 1},        // Never zero, which would mean "no expiry".
		{0, 0},
		{-time.Second, 0},
	}
	for _, tc := range tests {
		if got := expirySeconds(tc.in); got != tc.want {
			t.Errorf("expirySeconds(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestKeepaliveSeconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want uint16
	}{
		{30 * time.Second, 30},
		{0, 30},                           // Zero would disable keepalive entirely.
		{-time.Second, 30},                //
		{500 * time.Millisecond, 1},       // Sub-second still has to keep alive.
		{100 * time.Hour, math.MaxUint16}, // Clamped rather than overflowed.
	}
	for _, tc := range tests {
		if got := keepaliveSeconds(tc.in); got != tc.want {
			t.Errorf("keepaliveSeconds(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// By default the agent tries the broker at once, at start and after losing it,
// and then every interval, exactly: the backoff is off and so is its jitter
// (#13 Q3). autopaho asks for attempt 0 before the first attempt.
func TestReconnectEveryIntervalWithTheBackoffOff(t *testing.T) {
	delay := reconnectDelay(time.Second, false, time.Minute, 0.3)
	if got := delay(0); got != 0 {
		t.Errorf("wait before the first attempt = %s, want none", got)
	}
	for attempt := 1; attempt <= 20; attempt++ {
		if got := delay(attempt); got != time.Second {
			t.Errorf("wait before attempt %d = %s, want 1s", attempt, got)
		}
	}
}

// With the backoff on, the wait doubles after each failed attempt and stops at
// its ceiling, so that a broker that stays away is not tried every interval.
func TestReconnectBackoffGrowsAndIsBounded(t *testing.T) {
	delay := reconnectDelay(time.Second, true, 8*time.Second, 0)
	want := []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for attempt, w := range want {
		if got := delay(attempt); got != w {
			t.Errorf("wait before attempt %d = %s, want %s", attempt, got, w)
		}
	}
}

// Jitter is what keeps a site full of stations from reconnecting in lockstep
// after a broker restart, so with the backoff on it has to vary, and stay in
// range.
func TestReconnectBackoffJitterVariesWithinBounds(t *testing.T) {
	const jitter = 0.3
	delay := reconnectDelay(time.Second, true, time.Minute, jitter)

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		got := delay(1)
		if got < time.Duration(float64(time.Second)*(1-jitter)) || got > time.Duration(float64(time.Second)*(1+jitter)) {
			t.Fatalf("wait = %s, outside 1s +/- %v%%", got, jitter*100)
		}
		seen[got] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct waits in 50 draws; the jitter is not spreading reconnects", len(seen))
	}
}

func TestTLSConfigPlaintextSchemeHasNoTLS(t *testing.T) {
	u, err := url.Parse("tcp://localhost:1883")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg, err := tlsConfig(u, "", true)
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg != nil {
		t.Error("a plaintext URL produced a TLS configuration")
	}
}

// A CA file with a plaintext URL means someone believes the connection is
// encrypted when it is not. That is worth failing over rather than ignoring.
func TestTLSConfigRejectsCAFileOnPlaintextURL(t *testing.T) {
	u, _ := url.Parse("tcp://localhost:1883")
	_, err := tlsConfig(u, "/etc/skuhus-device-agent/ca.pem", false)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "not TLS") {
		t.Errorf("error = %v, want it to say the scheme is not TLS", err)
	}
}

func TestTLSConfigUsesHostnameAndRejectsBadCA(t *testing.T) {
	u, _ := url.Parse("tls://mq.internal:8883")
	cfg, err := tlsConfig(u, "", false)
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg.ServerName != "mq.internal" {
		t.Errorf("ServerName = %q, want mq.internal", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify is set without broker.insecure")
	}
	if cfg.MinVersion != 0x0303 {
		t.Errorf("MinVersion = %#x, want TLS 1.2", cfg.MinVersion)
	}

	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(notPEM, []byte("this is not a certificate"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := tlsConfig(u, notPEM, false); err == nil {
		t.Error("expected an error for a file containing no certificates")
	}
}

func TestDialRejectsUnusableOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{"no client id", Options{URL: "tcp://localhost:1883"}, "client id is required"},
		{"unparseable url", Options{URL: "://nope", ClientID: "pack-03"}, "broker url"},
		{
			"missing ca file",
			Options{URL: "tls://mq.internal:8883", ClientID: "pack-03", CAFile: "/nonexistent/ca.pem"},
			"ca_file",
		},
		{"will without a topic", Options{URL: "tcp://localhost:1883", ClientID: "pack-03", Will: func() ([]byte, error) { return []byte("{}"), nil }}, "a will needs both"},
		{"will topic without a payload", Options{URL: "tcp://localhost:1883", ClientID: "pack-03", WillTopic: "t"}, "a will needs both"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := Dial(ctx, tc.opts)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// The will is the offline message on the agent's status topic: QoS 1, not
// retained, published without delay, and composed afresh for every connection
// attempt, so that after a reconnection it says when that connection was made.
// The settings beside it are the measured ones the connection depends on: clean
// start, no session, no publish queue.
func TestConnectionConfiguration(t *testing.T) {
	brokerURL, _ := url.Parse("tcp://skuhus-dev-rabbitmq:1883")
	composed := 0
	cfg := clientConfig(Options{
		ClientID:  "pack-03",
		WillTopic: "skuhus/acme/vasby/pack-03/agent/pack-03/status",
		Will: func() ([]byte, error) {
			composed++
			return []byte(fmt.Sprintf(`{"kind":"offline","reason":"will","attempt":%d}`, composed)), nil
		},
	}, brokerURL, nil, slog.New(slog.DiscardHandler), &lineGate{})

	if cfg.WillMessage == nil || cfg.WillProperties == nil || cfg.ConnectPacketBuilder == nil {
		t.Fatal("no will registered")
	}
	if cfg.WillProperties.WillDelayInterval == nil || *cfg.WillProperties.WillDelayInterval != 0 {
		t.Errorf("will properties = %+v, want an immediate will", cfg.WillProperties)
	}
	// Two connection attempts, each through the builder autopaho calls, with the
	// packet holding the configured will by pointer as autopaho's does.
	for attempt := 1; attempt <= 2; attempt++ {
		packet, err := cfg.ConnectPacketBuilder(&paho.Connect{WillMessage: cfg.WillMessage, WillProperties: cfg.WillProperties}, brokerURL)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		message := packet.WillMessage
		want := fmt.Sprintf(`{"kind":"offline","reason":"will","attempt":%d}`, attempt)
		if message.Topic != "skuhus/acme/vasby/pack-03/agent/pack-03/status" || message.QoS != 1 || message.Retain || string(message.Payload) != want {
			t.Errorf("attempt %d: will = topic %q, QoS %d, retain %t, payload %s; want payload %s",
				attempt, message.Topic, message.QoS, message.Retain, message.Payload, want)
		}
	}
	if cfg.WillMessage.Payload != nil {
		t.Errorf("the configured will was changed to %s; each packet should get a copy", cfg.WillMessage.Payload)
	}
	if !cfg.CleanStartOnInitialConnection || cfg.SessionExpiryInterval != 0 || cfg.Queue != nil {
		t.Errorf("clean start %t, session expiry %d, queue %v: want a clean connection with no session and no queue",
			cfg.CleanStartOnInitialConnection, cfg.SessionExpiryInterval, cfg.Queue)
	}

	failing := clientConfig(Options{ClientID: "pack-03", WillTopic: "t", Will: func() ([]byte, error) { return nil, errors.New("encoder broke") }},
		brokerURL, nil, slog.New(slog.DiscardHandler), &lineGate{})
	if _, err := failing.ConnectPacketBuilder(&paho.Connect{WillMessage: failing.WillMessage}, brokerURL); err == nil || !strings.Contains(err.Error(), "compose will: encoder broke") {
		t.Errorf("a will that cannot be composed: err = %v, want the connection attempt to fail naming it", err)
	}

	if withoutWill := clientConfig(Options{ClientID: "pack-03"}, brokerURL, nil, slog.New(slog.DiscardHandler), &lineGate{}); withoutWill.WillMessage != nil || withoutWill.ConnectPacketBuilder != nil {
		t.Error("a will was registered without one being asked for")
	}
}

// paho's lines name the paho code that wrote them, not the adapter that passes
// them on: a source naming the adapter on every paho line identifies nothing.
// Here the caller is this test.
func TestPahoLinesNameTheirCaller(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	lines := &lineGate{}
	adapter := logAdapter{log: log.With("component", "paho"), level: slog.LevelDebug, lines: lines}
	_, _, line, _ := runtime.Caller(0)
	adapter.Printf("sending %s", "CONNECT")
	adapter.Println("connected")
	// After Close, paho goes on logging about a connection the agent closed
	// on purpose, after the log file is closed; those lines are discarded.
	lines.close()
	adapter.Println("handleError received extra error: EOF")

	records := logged.Records(t)
	if len(records) != 2 || records[0]["msg"] != "sending CONNECT" || records[1]["msg"] != "connected" {
		t.Fatalf("records = %v", records)
	}
	for i, record := range records {
		source := record["source"].(map[string]any)
		if !strings.HasSuffix(source["function"].(string), ".TestPahoLinesNameTheirCaller") || source["line"] != float64(line+1+i) {
			t.Errorf("record %d source = %v, want this test, line %d", i, source, line+1+i)
		}
	}
}

// The connection's own lines carry what they are about once, next to the
// broker and client id every one of them carries.
func TestConnectionLinesReportEachTransition(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	brokerURL, _ := url.Parse("tcp://broker:1883")
	cfg := clientConfig(Options{ClientID: "pack-03"}, brokerURL, nil, log.With("broker", brokerURL.Redacted(), "client_id", "pack-03"), &lineGate{})
	cfg.OnConnectionUp(nil, &paho.Connack{})
	cfg.OnConnectionDown()
	cfg.OnConnectError(errors.New("connection refused"))

	for msg, level := range map[string]string{
		"broker connected":                     "INFO",
		"broker connection lost, reconnecting": "WARN",
		"broker connection attempt failed":     "WARN",
	} {
		records := logged.WithMessage(t, msg)
		if len(records) != 1 || records[0]["level"] != level || records[0]["client_id"] != "pack-03" {
			t.Errorf("%q records = %v, want one at %s with the client id", msg, records, level)
		}
	}
}

// A tx's expiry is what was left of its MQTT message expiry when it arrived,
// and a tx without one says so rather than reading as expired.
func TestMessagesCarryTheirExpiry(t *testing.T) {
	received := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	seconds := uint32(27)
	with := messageOf(&paho.Publish{Topic: "skuhus/acme/vasby/pack-03/printer-1/tx", Payload: []byte("{}"),
		Properties: &paho.PublishProperties{MessageExpiry: &seconds}}, received)
	if !with.HasExpiry || with.Expiry != 27*time.Second || with.Received != received || with.Topic != "skuhus/acme/vasby/pack-03/printer-1/tx" {
		t.Errorf("message = %+v, want a 27 s expiry from %s", with, received)
	}
	without := messageOf(&paho.Publish{Topic: "t", Payload: []byte("{}")}, received)
	if without.HasExpiry || without.Expiry != 0 {
		t.Errorf("message without an expiry = %+v", without)
	}
}

// Every publish the connection receives reaches OnMessage, and is marked
// handled.
func TestReceivedPublishesReachOnMessage(t *testing.T) {
	brokerURL, _ := url.Parse("tcp://skuhus-dev-rabbitmq:1883")
	var got []Message
	cfg := clientConfig(Options{ClientID: "pack-03", OnMessage: func(message Message) { got = append(got, message) }},
		brokerURL, nil, slog.New(slog.DiscardHandler), &lineGate{})
	if len(cfg.OnPublishReceived) != 1 {
		t.Fatalf("%d publish handlers, want 1", len(cfg.OnPublishReceived))
	}
	handled, err := cfg.OnPublishReceived[0](paho.PublishReceived{Packet: &paho.Publish{Topic: "a/tx", Payload: []byte("job")}})
	if !handled || err != nil {
		t.Errorf("handler returned %t, %v; want handled", handled, err)
	}
	if len(got) != 1 || got[0].Topic != "a/tx" || string(got[0].Payload) != "job" {
		t.Errorf("OnMessage got %+v", got)
	}
	none := clientConfig(Options{ClientID: "pack-03"}, brokerURL, nil, slog.New(slog.DiscardHandler), &lineGate{})
	if len(none.OnPublishReceived) != 0 {
		t.Errorf("a connection without OnMessage has %d publish handlers", len(none.OnPublishReceived))
	}
}
