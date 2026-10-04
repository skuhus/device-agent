package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// QoS levels, as DESIGN-V2.md, "Message formats", assigns them.
const (
	// qosAtLeastOnce is used for readings, events, tx results and the offline
	// message. The broker measured in docs/spikes/m0-mqtt5.md offers no QoS 2,
	// and every message carries an id a consumer drops a redelivery by.
	qosAtLeastOnce = 1
	// qosAtMostOnce is used for keepalives: the next one is an interval away
	// and carries the same counters, so a redelivery guarantee buys nothing.
	qosAtMostOnce = 0
)

// Options configures the connection. Everything here comes from the broker
// section of the configuration, except the will, which the caller builds from
// the station identity.
type Options struct {
	URL      string
	ClientID string
	Username string
	Password string
	CAFile   string
	// Insecure allows a plaintext URL and skips certificate verification. The
	// configuration layer already refuses a plaintext URL without it.
	Insecure  bool
	Keepalive time.Duration

	// ReconnectInterval is the wait between attempts to connect. With
	// ReconnectBackoff the wait doubles after each failed attempt, up to
	// BackoffMax, and is spread by BackoffJitter, a fraction either way.
	ReconnectInterval time.Duration
	ReconnectBackoff  bool
	BackoffMax        time.Duration
	BackoffJitter     float64

	// Will composes the offline message the broker publishes on WillTopic if
	// this agent stops without disconnecting. It is called for every connection
	// attempt, because the broker takes the will in the CONNECT packet: a will
	// composed once would carry the first connection's time after every
	// reconnection. It is not retained: nothing the agent publishes is
	// (#11 Q1), and RabbitMQ does not retain a will in any case.
	WillTopic string
	Will      func() ([]byte, error)

	// Subscriptions are the topics subscribed to at QoS 1 on every
	// connection: each starts clean, so the broker keeps no subscription
	// across a reconnect. OnMessage receives what arrives on them. It is
	// called on paho's own goroutine, one message after another, and must not
	// block: hand the message on.
	Subscriptions []string
	OnMessage     func(Message)

	Logger *slog.Logger
	// OnUp and OnDown report connection transitions. They are called from the
	// connection manager's own goroutine and must not block: hand the event to
	// a channel rather than publishing from inside them.
	OnUp   func()
	OnDown func()
}

// Message is a publish that arrived on a subscription.
type Message struct {
	Topic   string
	Payload []byte
	// Expiry is what remained of the message's expiry when it arrived, and
	// HasExpiry whether it had one at all.
	Expiry    time.Duration
	HasExpiry bool
	Received  time.Time
}

// Client is the agent's connection to the broker.
type Client struct {
	cm  *autopaho.ConnectionManager
	log *slog.Logger
	// lines closes once the connection is closed; paho's lines after it are
	// discarded (see logAdapter).
	lines *lineGate
}

// Dial builds the connection manager and starts connecting. It returns as soon
// as the manager exists, without waiting for a connection: a broker that is
// down at startup is an expected condition, not a configuration error, and the
// agent still has a device to open and a status to report once it is up. Use
// AwaitConnection to wait.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	if opts.ClientID == "" {
		return nil, errors.New("mqtt: client id is required")
	}
	brokerURL, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("mqtt: broker url %q: %w", opts.URL, err)
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("broker", brokerURL.Redacted(), "client_id", opts.ClientID)

	tlsCfg, err := tlsConfig(brokerURL, opts.CAFile, opts.Insecure)
	if err != nil {
		return nil, err
	}
	if (opts.Will == nil) != (opts.WillTopic == "") {
		return nil, errors.New("mqtt: a will needs both a topic and a way to compose it")
	}

	lines := &lineGate{}
	cm, err := autopaho.NewConnection(ctx, clientConfig(opts, brokerURL, tlsCfg, log, lines))
	if err != nil {
		return nil, fmt.Errorf("mqtt: %w", err)
	}
	return &Client{cm: cm, log: log, lines: lines}, nil
}

// clientConfig is the connection's whole configuration, built apart from Dial
// so that a test can check it without a broker.
func clientConfig(opts Options, brokerURL *url.URL, tlsCfg *tls.Config, log *slog.Logger, lines *lineGate) autopaho.ClientConfig {
	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{brokerURL},
		TlsCfg:     tlsCfg,
		KeepAlive:  keepaliveSeconds(opts.Keepalive),
		// Section 5.2: clean start, session expiry zero. A reconnecting agent
		// starts fresh because a queued stale command is harmful, not useful.
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ConnectUsername:               opts.Username,
		ConnectPassword:               []byte(opts.Password),
		ReconnectBackoff:              reconnectDelay(opts.ReconnectInterval, opts.ReconnectBackoff, opts.BackoffMax, opts.BackoffJitter),
		ConnectTimeout:                10 * time.Second,
		// Queue stays nil on purpose. With a queue, a publish while
		// disconnected is accepted and sent later, which is exactly the
		// offline replay section 6 forbids. Nil makes it fail immediately.
		Queue: nil,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
			log.Info("broker connected", "session_present", connack.SessionPresent)
			if len(opts.Subscriptions) > 0 {
				// Subscribing waits for the broker's answer, and this must
				// not block the connection manager.
				go subscribe(cm, opts.Subscriptions, log)
			}
			if opts.OnUp != nil {
				opts.OnUp()
			}
		},
		OnConnectionDown: func() bool {
			log.Warn("broker connection lost, reconnecting")
			if opts.OnDown != nil {
				opts.OnDown()
			}
			return true
		},
		OnConnectError: func(err error) {
			log.Warn("broker connection attempt failed", "error", err.Error())
		},
		Errors:     logAdapter{log: log, level: slog.LevelWarn, lines: lines},
		Debug:      logAdapter{log: log, level: slog.LevelDebug, lines: lines},
		PahoErrors: logAdapter{log: log.With("component", "paho"), level: slog.LevelWarn, lines: lines},
		PahoDebug:  logAdapter{log: log.With("component", "paho"), level: slog.LevelDebug, lines: lines},
		ClientConfig: paho.ClientConfig{
			ClientID: opts.ClientID,
		},
	}
	if opts.OnMessage != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(received paho.PublishReceived) (bool, error) {
				opts.OnMessage(messageOf(received.Packet, time.Now()))
				return true, nil
			},
		}
	}
	if opts.Will != nil {
		var noDelay uint32 // Publish the will immediately; a delay only hides a death.
		cfg.WillMessage = &paho.WillMessage{Topic: opts.WillTopic, QoS: qosAtLeastOnce}
		cfg.WillProperties = &paho.WillProperties{WillDelayInterval: &noDelay, ContentType: "application/json"}
		// autopaho builds a CONNECT packet for every attempt, and passes it here
		// before sending it (autopaho/net.go, buildConnectPacket). The packet
		// holds the configured WillMessage by pointer, so the payload goes into
		// a copy rather than into the shared message.
		cfg.ConnectPacketBuilder = func(packet *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			payload, err := opts.Will()
			if err != nil {
				return nil, fmt.Errorf("compose will: %w", err)
			}
			will := *packet.WillMessage
			will.Payload = payload
			packet.WillMessage = &will
			return packet, nil
		}
	}
	return cfg
}

// subscribe asks for every topic at QoS 1 and logs what the broker answered.
// A refused topic is an ERROR: its device will never receive a tx, and the
// usual cause, the station's topic permission, is the operator's to fix.
func subscribe(cm *autopaho.ConnectionManager, topics []string, log *slog.Logger) {
	subscriptions := make([]paho.SubscribeOptions, 0, len(topics))
	for _, topic := range topics {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: topic, QoS: qosAtLeastOnce})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	suback, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil {
		log.Error("subscribing failed; no tx will arrive until the next connection", "topics", topics, "error", err.Error())
		return
	}
	for i, topic := range topics {
		if i >= len(suback.Reasons) {
			log.Error("the broker answered fewer subscriptions than were asked for", "topic", topic, "answers", len(suback.Reasons))
			continue
		}
		if code := suback.Reasons[i]; code >= 0x80 {
			log.Error("subscription refused; this device will receive no tx", "topic", topic, "reason", fmt.Sprintf("0x%02x", code))
			continue
		}
		log.Info("subscribed", "topic", topic, "qos", suback.Reasons[i])
	}
}

// messageOf takes what the agent needs from a received publish.
func messageOf(packet *paho.Publish, received time.Time) Message {
	message := Message{Topic: packet.Topic, Payload: packet.Payload, Received: received}
	if packet.Properties != nil && packet.Properties.MessageExpiry != nil {
		message.HasExpiry = true
		message.Expiry = time.Duration(*packet.Properties.MessageExpiry) * time.Second
	}
	return message
}

// AwaitConnection blocks until the connection is up or ctx ends.
func (client *Client) AwaitConnection(ctx context.Context) error {
	return client.cm.AwaitConnection(ctx)
}

// PublishRx publishes one reading at QoS 1, with the device's message expiry.
//
// The expiry is what makes a reading perishable: the broker discards one that
// outlived it, so no consumer has to decide whether an old reading still
// means anything. It returns an error when the broker did not acknowledge
// within ctx, which the caller records as a failed delivery rather than
// retrying: by the time a retry lands, the reading is stale anyway.
func (client *Client) PublishRx(ctx context.Context, topic string, payload []byte, expiry time.Duration) error {
	seconds := expirySeconds(expiry)
	return client.publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        qosAtLeastOnce,
		Payload:    payload,
		Properties: &paho.PublishProperties{MessageExpiry: &seconds, ContentType: "application/json"},
	})
}

// PublishEvent publishes a device event at QoS 1, with the device's message
// expiry, as DESIGN-V2.md, "Publishing", assigns it. Like a reading, an event
// is not retried: the keepalive counts what it reported either way.
func (client *Client) PublishEvent(ctx context.Context, topic string, payload []byte, expiry time.Duration) error {
	seconds := expirySeconds(expiry)
	return client.publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        qosAtLeastOnce,
		Payload:    payload,
		Properties: &paho.PublishProperties{MessageExpiry: &seconds, ContentType: "application/json"},
	})
}

// PublishKeepalive publishes a keepalive at QoS 0. Its expiry is the keepalive's
// gone_after_s, because a keepalive older than that says nothing true. At QoS 0
// nothing is acknowledged, so a nil error means the packet was written, not
// that the broker took it.
func (client *Client) PublishKeepalive(ctx context.Context, topic string, payload []byte, expiry time.Duration) error {
	seconds := expirySeconds(expiry)
	return client.publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        qosAtMostOnce,
		Payload:    payload,
		Properties: &paho.PublishProperties{MessageExpiry: &seconds, ContentType: "application/json"},
	})
}

// PublishOffline publishes the agent's offline message at QoS 1, with no
// expiry, as the last thing before a clean disconnect.
func (client *Client) PublishOffline(ctx context.Context, topic string, payload []byte) error {
	return client.publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        qosAtLeastOnce,
		Payload:    payload,
		Properties: &paho.PublishProperties{ContentType: "application/json"},
	})
}

// Close publishes nothing. The caller publishes its offline message first, then
// calls this, so that the broker sees a clean DISCONNECT and discards the will.
func (client *Client) Close(ctx context.Context) error {
	err := client.cm.Disconnect(ctx)
	client.lines.close()
	if err != nil {
		return fmt.Errorf("mqtt: disconnect: %w", err)
	}
	return nil
}

// publish sends one packet and insists on the acknowledgement. Section 10 says
// not to assume a publish succeeded because the call returned: at QoS 1 the
// PUBACK carries a reason code, and a code of 0x80 or above is a refusal, most
// often a topic the station's credentials do not authorise.
func (client *Client) publish(ctx context.Context, packet *paho.Publish) error {
	resp, err := client.cm.Publish(ctx, packet)
	if err != nil {
		if errors.Is(err, autopaho.ConnectionDownError) {
			return fmt.Errorf("publish to %s: broker connection is down", packet.Topic)
		}
		return fmt.Errorf("publish to %s: %w", packet.Topic, err)
	}
	// QoS 0 has nothing to acknowledge.
	if resp == nil || packet.QoS == qosAtMostOnce {
		return nil
	}
	if resp.ReasonCode >= 0x80 {
		return fmt.Errorf("publish to %s refused with reason 0x%02x%s", packet.Topic, resp.ReasonCode, reasonString(resp))
	}
	return nil
}

func reasonString(resp *paho.PublishResponse) string {
	if resp.Properties != nil && resp.Properties.ReasonString != "" {
		return ": " + resp.Properties.ReasonString
	}
	return ""
}

// expirySeconds rounds up, so a sub-second ttl expires after a second rather
// than immediately. MQTT expresses expiry in whole seconds.
func expirySeconds(delay time.Duration) uint32 {
	if delay <= 0 {
		return 0
	}
	seconds := math.Ceil(delay.Seconds())
	if seconds > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(seconds)
}

// keepaliveSeconds converts to the protocol's unit. Zero disables keepalive
// entirely, which would leave a half-open connection undetected, so a
// non-positive value falls back to the specification's 30 seconds.
func keepaliveSeconds(delay time.Duration) uint16 {
	if delay <= 0 {
		return 30
	}
	seconds := math.Round(delay.Seconds())
	if seconds > math.MaxUint16 {
		return math.MaxUint16
	}
	if seconds < 1 {
		return 1
	}
	return uint16(seconds)
}

// reconnectDelay returns autopaho's wait before each attempt to connect.
// autopaho asks for attempt 0 before the first attempt, at start and after a
// connection is lost, and that one goes at once (autopaho/backoff.go, Backoff).
// After a failed attempt the wait is the interval, every time, unless grow is
// set (#13 Q3). With grow it doubles after each failed attempt up to maxDelay,
// with proportional jitter, so that stations that lost the broker together do
// not retry in lockstep.
func reconnectDelay(interval time.Duration, grow bool, maxDelay time.Duration, jitter float64) func(int) time.Duration {
	if interval <= 0 {
		interval = time.Second
	}
	if maxDelay < interval {
		maxDelay = interval
	}
	return func(attempt int) time.Duration {
		if attempt <= 0 {
			return 0
		}
		if !grow {
			return interval
		}
		delay := interval
		for i := 1; i < attempt && delay < maxDelay; i++ {
			delay *= 2
		}
		delay = min(delay, maxDelay)
		if jitter <= 0 {
			return delay
		}
		spread := float64(delay) * jitter
		return time.Duration(float64(delay) - spread + rand.Float64()*2*spread)
	}
}

// tlsConfig builds the TLS settings for a TLS scheme, and returns nil for a
// plaintext one so autopaho dials TCP.
func tlsConfig(brokerURL *url.URL, caFile string, insecure bool) (*tls.Config, error) {
	switch brokerURL.Scheme {
	case "tls", "ssl", "mqtts", "mqtt+ssl", "tcps", "wss":
	default:
		if caFile != "" {
			return nil, fmt.Errorf("mqtt: broker.ca_file is set but the URL scheme %q is not TLS", brokerURL.Scheme)
		}
		return nil, nil
	}

	cfg := &tls.Config{
		ServerName:         brokerURL.Hostname(),
		InsecureSkipVerify: insecure,
		MinVersion:         tls.VersionTLS12,
	}
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("mqtt: broker.ca_file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mqtt: broker.ca_file %s contains no certificates", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// logAdapter bridges paho's logger interface to slog. paho logs connection
// detail that is worth having when a station will not connect, and worth
// keeping out of the way otherwise.
//
// Each record's source is the paho code that called it, not this adapter: a
// source that names the adapter on every paho line identifies nothing.
//
// Once Close has returned, paho's lines are discarded. autopaho cannot tell
// when paho has finished shutting down (autopaho/auto.go, Done), and its
// goroutines go on logging about the connection the agent just closed, after
// the agent has closed its log file. Measured: one DEBUG line per clean stop,
// "handleError received extra error", a few milliseconds after the file closed.
type logAdapter struct {
	log   *slog.Logger
	level slog.Level
	lines *lineGate
}

func (adapter logAdapter) Println(v ...any) {
	adapter.emit(fmt.Sprint(v...))
}

func (adapter logAdapter) Printf(format string, v ...any) {
	adapter.emit(fmt.Sprintf(format, v...))
}

// emit writes one record with the caller of Println or Printf as its source,
// as log/slog's documentation shows for wrapping its output methods.
func (adapter logAdapter) emit(msg string) {
	ctx := context.Background()
	if !adapter.log.Enabled(ctx, adapter.level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip Callers, emit, and Println or Printf
	record := slog.NewRecord(time.Now(), adapter.level, msg, pcs[0])
	adapter.lines.pass(func() {
		// The agent's handler reports a record it could not write itself.
		_ = adapter.log.Handler().Handle(ctx, record)
	})
}

// lineGate lets paho's lines through until the connection is closed. A line
// holds it while it is written, so close waits for a line already on its way:
// checking a flag first and writing after leaves a window, measured at 2 ms,
// in which a line written after the log file closed still gets through.
type lineGate struct {
	mu     sync.RWMutex
	closed bool
}

func (gate *lineGate) pass(write func()) {
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	if !gate.closed {
		write()
	}
}

func (gate *lineGate) close() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.closed = true
}
