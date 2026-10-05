package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/skuhus/device-agent/internal/backoff"
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

// reasonCodeFailure is the lowest MQTT 5 reason code that reports a failure
// (MQTT 5.0, section 2.4).
const reasonCodeFailure = 0x80

// callersAboveEmit is how many frames emit skips to find the line that logged:
// runtime.Callers, emit itself, and logAdapter's Println or Printf.
const callersAboveEmit = 3

// Options configures the connection. Everything here comes from the broker
// section of the configuration, except the will, which the caller builds from
// the station identity.
type Options struct {
	URL      string
	ClientID string
	Username string
	Password string
	// TLS says whether the connection is encrypted; the configuration decides
	// it from the URL's scheme. CAFile and Insecure apply only with it.
	TLS    bool
	CAFile string
	// Insecure skips certificate verification. The configuration refuses a
	// plaintext URL without it.
	Insecure bool
	// Keepalive must be positive: zero would turn keepalive off, and a
	// half-open connection would go undetected.
	Keepalive time.Duration
	// ConnectTimeout bounds each attempt to connect, from dialling to the
	// broker's CONNACK, and the wait for the answer to the subscriptions.
	ConnectTimeout time.Duration

	// Reconnect is the wait after a failed attempt to connect. The first
	// attempt, at start and after a connection is lost, goes at once.
	Reconnect backoff.Policy

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
	if opts.Keepalive <= 0 {
		return nil, fmt.Errorf("mqtt: keepalive must be positive, got %s", opts.Keepalive)
	}
	if opts.ConnectTimeout <= 0 {
		return nil, fmt.Errorf("mqtt: connect timeout must be positive, got %s", opts.ConnectTimeout)
	}
	if err := opts.Reconnect.Validate(); err != nil {
		return nil, fmt.Errorf("mqtt: reconnect: %w", err)
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

	var tlsCfg *tls.Config
	switch {
	case opts.TLS:
		if tlsCfg, err = tlsConfig(brokerURL.Hostname(), opts.CAFile, opts.Insecure); err != nil {
			return nil, err
		}
	case opts.CAFile != "":
		return nil, errors.New("mqtt: a CA file is set but the connection is not TLS")
	}
	if (opts.Will == nil) != (opts.WillTopic == "") {
		return nil, errors.New("mqtt: a will needs both a topic and a way to compose it")
	}

	log.Info("broker connection settings", "tls", opts.TLS, "ca_file", opts.CAFile, "insecure", opts.Insecure,
		"keepalive", opts.Keepalive.String(), "connect_timeout", opts.ConnectTimeout.String(),
		"reconnect_interval", opts.Reconnect.Interval.String(), "reconnect_backoff", opts.Reconnect.Grow,
		"reconnect_backoff_max", opts.Reconnect.Max.String(), "reconnect_backoff_jitter", opts.Reconnect.Jitter,
		"subscriptions", opts.Subscriptions, "will_topic", opts.WillTopic)
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
		ReconnectBackoff:              reconnectDelay(opts.Reconnect),
		ConnectTimeout:                opts.ConnectTimeout,
		// Queue stays nil on purpose. With a queue, a publish while
		// disconnected is accepted and sent later, which is exactly the
		// offline replay section 6 forbids. Nil makes it fail immediately.
		Queue: nil,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
			log.Info("broker connected", "session_present", connack.SessionPresent)
			if len(opts.Subscriptions) > 0 {
				// Subscribing waits for the broker's answer, and this must
				// not block the connection manager.
				go subscribe(cm, opts.Subscriptions, opts.ConnectTimeout, log)
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
				opts.OnMessage(messageFromPublish(received.Packet, time.Now()))
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

// subscriber is the part of the connection manager that subscribe uses, so
// that a test can answer for the broker.
type subscriber interface {
	Subscribe(ctx context.Context, packet *paho.Subscribe) (*paho.Suback, error)
}

// subscribe asks for every topic at QoS 1, waiting at most timeout for the
// answer, and logs what the broker answered. A refused topic is an ERROR: its
// device will never receive a tx, and the usual cause, the station's topic
// permission, is the operator's to fix.
func subscribe(cm subscriber, topics []string, timeout time.Duration, log *slog.Logger) {
	subscriptions := make([]paho.SubscribeOptions, 0, len(topics))
	for _, topic := range topics {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: topic, QoS: qosAtLeastOnce})
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	suback, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil {
		log.Error("subscribing failed; no tx will arrive until the next connection", "topics", topics,
			"timeout", timeout.String(), "error", err.Error())
		return
	}
	for index, topic := range topics {
		if index >= len(suback.Reasons) {
			log.Error("the broker answered fewer subscriptions than were asked for", "topic", topic, "answers", len(suback.Reasons))
			continue
		}
		if code := suback.Reasons[index]; code >= reasonCodeFailure {
			log.Error("subscription refused; this device will receive no tx", "topic", topic, "reason", fmt.Sprintf("0x%02x", code))
			continue
		}
		log.Info("subscribed", "topic", topic, "qos", suback.Reasons[index])
	}
}

// messageFromPublish takes what the agent needs from a received publish.
func messageFromPublish(packet *paho.Publish, received time.Time) Message {
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
	seconds := messageExpiryInterval(expiry)
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
	seconds := messageExpiryInterval(expiry)
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
	seconds := messageExpiryInterval(expiry)
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
	if resp.ReasonCode >= reasonCodeFailure {
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

// messageExpiryInterval is MQTT's Message Expiry Interval for expiry, in
// whole seconds, the protocol's unit. It rounds up, so that a sub-second
// expiry ends after a second rather than at once, and is 0, meaning none, for
// no expiry.
func messageExpiryInterval(expiry time.Duration) uint32 {
	if expiry <= 0 {
		return 0
	}
	seconds := math.Ceil(expiry.Seconds())
	if seconds > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(seconds)
}

// keepaliveSeconds converts a positive keepalive to whole seconds, the
// protocol's unit: rounded, at least 1, because 0 would turn keepalive off,
// and at most the field's maximum.
func keepaliveSeconds(delay time.Duration) uint16 {
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
// Attempt n after it is retry n of the policy.
func reconnectDelay(policy backoff.Policy) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		if attempt <= 0 {
			return 0
		}
		return policy.Wait(attempt)
	}
}

// tlsConfig builds the TLS settings for a connection to host, verified against
// caFile when there is one and the system's CA certificates otherwise.
func tlsConfig(host, caFile string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{
		ServerName:         host,
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

func (adapter logAdapter) Println(values ...any) {
	adapter.emit(fmt.Sprint(values...))
}

func (adapter logAdapter) Printf(format string, values ...any) {
	adapter.emit(fmt.Sprintf(format, values...))
}

// emit writes one record with the caller of Println or Printf as its source,
// as log/slog's documentation shows for wrapping its output methods.
func (adapter logAdapter) emit(msg string) {
	ctx := context.Background()
	if !adapter.log.Enabled(ctx, adapter.level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(callersAboveEmit, pcs[:])
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
