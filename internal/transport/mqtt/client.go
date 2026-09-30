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

	BackoffInitial time.Duration
	BackoffMax     time.Duration
	BackoffJitter  float64

	// Will composes the offline message the broker publishes on WillTopic if
	// this agent stops without disconnecting. It is called for every connection
	// attempt, because the broker takes the will in the CONNECT packet: a will
	// composed once would carry the first connection's time after every
	// reconnection. It is not retained: nothing the agent publishes is
	// (#11 Q1), and RabbitMQ does not retain a will in any case.
	WillTopic string
	Will      func() ([]byte, error)

	Logger *slog.Logger
	// OnUp and OnDown report connection transitions. They are called from the
	// connection manager's own goroutine and must not block: hand the event to
	// a channel rather than publishing from inside them.
	OnUp   func()
	OnDown func()
}

// Client is the agent's connection to the broker.
type Client struct {
	cm  *autopaho.ConnectionManager
	log *slog.Logger
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

	cm, err := autopaho.NewConnection(ctx, clientConfig(opts, brokerURL, tlsCfg, log))
	if err != nil {
		return nil, fmt.Errorf("mqtt: %w", err)
	}
	return &Client{cm: cm, log: log}, nil
}

// clientConfig is the connection's whole configuration, built apart from Dial
// so that a test can check it without a broker.
func clientConfig(opts Options, brokerURL *url.URL, tlsCfg *tls.Config, log *slog.Logger) autopaho.ClientConfig {
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
		ReconnectBackoff:              backoff(opts.BackoffInitial, opts.BackoffMax, opts.BackoffJitter),
		ConnectTimeout:                10 * time.Second,
		// Queue stays nil on purpose. With a queue, a publish while
		// disconnected is accepted and sent later, which is exactly the
		// offline replay section 6 forbids. Nil makes it fail immediately.
		Queue: nil,
		OnConnectionUp: func(_ *autopaho.ConnectionManager, connack *paho.Connack) {
			log.Info("broker connected", "session_present", connack.SessionPresent)
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
		Errors:     logAdapter{log: log, level: slog.LevelWarn},
		Debug:      logAdapter{log: log, level: slog.LevelDebug},
		PahoErrors: logAdapter{log: log.With("component", "paho"), level: slog.LevelWarn},
		PahoDebug:  logAdapter{log: log.With("component", "paho"), level: slog.LevelDebug},
		ClientConfig: paho.ClientConfig{
			ClientID: opts.ClientID,
		},
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
	if err := client.cm.Disconnect(ctx); err != nil {
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

// backoff returns autopaho's per-attempt delay: exponential from initial to
// max, with proportional jitter so a site full of stations does not reconnect
// in lockstep after a broker restart.
func backoff(initial, maxDelay time.Duration, jitter float64) func(int) time.Duration {
	if initial <= 0 {
		initial = time.Second
	}
	if maxDelay < initial {
		maxDelay = initial
	}
	return func(attempt int) time.Duration {
		delay := initial
		for i := 0; i < attempt && delay < maxDelay; i++ {
			delay *= 2
		}
		if delay > maxDelay {
			delay = maxDelay
		}
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
type logAdapter struct {
	log   *slog.Logger
	level slog.Level
}

func (adapter logAdapter) Println(v ...any) {
	adapter.log.Log(context.Background(), adapter.level, fmt.Sprint(v...))
}

func (adapter logAdapter) Printf(format string, v ...any) {
	adapter.log.Log(context.Background(), adapter.level, fmt.Sprintf(format, v...))
}
