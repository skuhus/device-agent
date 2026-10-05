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
	"github.com/skuhus/device-agent/internal/backoff"
	"github.com/skuhus/device-agent/internal/logging/logtest"
)

func TestMessageExpiryIntervalRoundsUp(t *testing.T) {
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
		if got := messageExpiryInterval(tc.in); got != tc.want {
			t.Errorf("messageExpiryInterval(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestKeepaliveSeconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want uint16
	}{
		{30 * time.Second, 30},
		{500 * time.Millisecond, 1},       // Sub-second still has to keep alive.
		{100 * time.Hour, math.MaxUint16}, // Clamped rather than overflowed.
	}
	for _, tc := range tests {
		if got := keepaliveSeconds(tc.in); got != tc.want {
			t.Errorf("keepaliveSeconds(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The agent tries the broker at once, at start and after losing it: autopaho
// asks for attempt 0 before the first attempt. After that, attempt n waits
// what the policy gives retry n, which internal/backoff tests.
func TestReconnectAtOnceThenAsThePolicySays(t *testing.T) {
	fixed := reconnectDelay(backoff.Policy{Interval: time.Second})
	growing := reconnectDelay(backoff.Policy{Interval: time.Second, Grow: true, Max: 8 * time.Second})
	for _, delay := range []func(int) time.Duration{fixed, growing} {
		if got := delay(0); got != 0 {
			t.Errorf("wait before the first attempt = %s, want none", got)
		}
	}
	for attempt := 1; attempt <= 5; attempt++ {
		if got := fixed(attempt); got != time.Second {
			t.Errorf("backoff off: wait before attempt %d = %s, want 1s", attempt, got)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for index, expected := range want {
		attempt := index + 1
		if got := growing(attempt); got != expected {
			t.Errorf("backoff on: wait before attempt %d = %s, want %s", attempt, got, expected)
		}
	}
}

func TestTLSConfigUsesHostnameAndRejectsBadCA(t *testing.T) {
	cfg, err := tlsConfig("mq.internal", "", false)
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
	if _, err := tlsConfig("mq.internal", notPEM, false); err == nil {
		t.Error("expected an error for a file containing no certificates")
	}
}

// Each case changes one thing in options Dial accepts, so that each refusal
// is for the reason the case names.
func TestDialRejectsUnusableOptions(t *testing.T) {
	usable := Options{
		URL: "tcp://localhost:1883", ClientID: "pack-03", Keepalive: 30 * time.Second, ConnectTimeout: 10 * time.Second,
		Reconnect: backoff.Policy{Interval: time.Second},
	}
	tests := []struct {
		name   string
		change func(*Options)
		want   string
	}{
		{"no client id", func(opts *Options) { opts.ClientID = "" }, "client id is required"},
		// Zero would turn keepalive off, and a half-open connection would go
		// undetected; the configuration refuses it, and so does Dial.
		{"no keepalive", func(opts *Options) { opts.Keepalive = 0 }, "keepalive must be positive, got 0s"},
		{"no connect timeout", func(opts *Options) { opts.ConnectTimeout = 0 }, "connect timeout must be positive, got 0s"},
		{"no reconnect interval", func(opts *Options) { opts.Reconnect = backoff.Policy{} }, "reconnect: the interval must be positive"},
		{"unparseable url", func(opts *Options) { opts.URL = "://nope" }, "broker url"},
		{"missing ca file", func(opts *Options) {
			opts.URL, opts.TLS, opts.CAFile = "tls://mq.internal:8883", true, "/nonexistent/ca.pem"
		}, "ca_file"},
		// A CA file without TLS means someone believes the connection is
		// encrypted when it is not.
		{"ca file without TLS", func(opts *Options) { opts.CAFile = "/etc/skuhus-device-agent/ca.pem" }, "a CA file is set but the connection is not TLS"},
		{"will without a topic", func(opts *Options) { opts.Will = func() ([]byte, error) { return []byte("{}"), nil } }, "a will needs both"},
		{"will topic without a payload", func(opts *Options) { opts.WillTopic = "t" }, "a will needs both"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := usable
			tc.change(&opts)
			_, err := Dial(ctx, opts)
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
		ClientID:       "pack-03",
		ConnectTimeout: 7 * time.Second,
		WillTopic:      "skuhus/acme/vasby/pack-03/agent/pack-03/status",
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
	if cfg.ConnectTimeout != 7*time.Second {
		t.Errorf("connect timeout = %s, want the configured 7s", cfg.ConnectTimeout)
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
	with := messageFromPublish(&paho.Publish{Topic: "skuhus/acme/vasby/pack-03/printer-1/tx", Payload: []byte("{}"),
		Properties: &paho.PublishProperties{MessageExpiry: &seconds}}, received)
	if !with.HasExpiry || with.Expiry != 27*time.Second || with.Received != received || with.Topic != "skuhus/acme/vasby/pack-03/printer-1/tx" {
		t.Errorf("message = %+v, want a 27 s expiry from %s", with, received)
	}
	without := messageFromPublish(&paho.Publish{Topic: "t", Payload: []byte("{}")}, received)
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

// fakeSubscriber answers a SUBSCRIBE as the broker would, and records the
// packet and how long it was given.
type fakeSubscriber struct {
	answer   *paho.Suback
	err      error
	packet   *paho.Subscribe
	deadline time.Duration
}

func (fake *fakeSubscriber) Subscribe(ctx context.Context, packet *paho.Subscribe) (*paho.Suback, error) {
	fake.packet = packet
	if deadline, ok := ctx.Deadline(); ok {
		fake.deadline = time.Until(deadline)
	}
	return fake.answer, fake.err
}

// Every tx topic is asked for at QoS 1 within the connect timeout. What the
// broker grants is INFO; a refused topic, or no answer at all, is an ERROR,
// because that device will receive no tx.
func TestSubscribeAsksForEachTopicAndLogsTheAnswer(t *testing.T) {
	topics := []string{"skuhus/acme/vasby/pack-03/printer-1/tx", "skuhus/acme/vasby/pack-03/printer-2/tx"}
	logger, log := logtest.New(t, "debug")
	broker := &fakeSubscriber{answer: &paho.Suback{Reasons: []byte{1, 0x87}}}
	subscribe(broker, topics, 7*time.Second, logger)

	if broker.deadline <= 6*time.Second || broker.deadline > 7*time.Second {
		t.Errorf("subscribing was given %s, want the 7s connect timeout", broker.deadline)
	}
	if len(broker.packet.Subscriptions) != 2 {
		t.Fatalf("asked for %d topics, want 2", len(broker.packet.Subscriptions))
	}
	for index, subscription := range broker.packet.Subscriptions {
		if subscription.Topic != topics[index] || subscription.QoS != 1 {
			t.Errorf("subscription %d = %s at QoS %d, want %s at 1", index, subscription.Topic, subscription.QoS, topics[index])
		}
	}
	granted := log.WithMessage(t, "subscribed")
	if len(granted) != 1 || granted[0]["topic"] != topics[0] || granted[0]["level"] != "INFO" {
		t.Errorf("subscribed records = %v, want one INFO for %s", granted, topics[0])
	}
	refused := log.WithMessage(t, "subscription refused; this device will receive no tx")
	if len(refused) != 1 || refused[0]["topic"] != topics[1] || refused[0]["reason"] != "0x87" || refused[0]["level"] != "ERROR" {
		t.Errorf("refused records = %v, want one ERROR for %s with reason 0x87", refused, topics[1])
	}

	failingLogger, failingLog := logtest.New(t, "debug")
	subscribe(&fakeSubscriber{err: context.DeadlineExceeded}, topics, 7*time.Second, failingLogger)
	failed := failingLog.WithMessage(t, "subscribing failed; no tx will arrive until the next connection")
	if len(failed) != 1 || failed[0]["level"] != "ERROR" || failed[0]["timeout"] != "7s" {
		t.Errorf("failure records = %v, want one ERROR with the 7s timeout", failed)
	}
}
