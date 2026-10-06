// Package mqttconn connects the development tools, dev/consumer and
// dev/sendtx, to a broker as one MQTT 5 client each, and subscribes them.
package mqttconn

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

const (
	// dialTimeout bounds reaching the broker.
	dialTimeout = 10 * time.Second
	// keepaliveSeconds is the MQTT keepalive the tools connect with.
	keepaliveSeconds = 30
	// reasonCodeFailure is the lowest MQTT 5 reason code that reports a
	// failure (MQTT 5.0, section 2.4).
	reasonCodeFailure = 0x80
)

// Connect dials broker, connects as config.ClientID with a clean start, with
// the credentials when username is set, and returns the client once the broker
// has accepted it. config's Conn is set here.
func Connect(ctx context.Context, broker, username, password string, config paho.ClientConfig) (*paho.Client, error) {
	conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", broker, err)
	}
	config.Conn = conn
	client := paho.NewClient(config)
	packet := &paho.Connect{ClientID: config.ClientID, KeepAlive: keepaliveSeconds, CleanStart: true}
	if username != "" {
		packet.Username, packet.UsernameFlag = username, true
		packet.Password, packet.PasswordFlag = []byte(password), true
	}
	connack, err := client.Connect(ctx, packet)
	if err != nil {
		return nil, fmt.Errorf("MQTT connect: %w", err)
	}
	if connack.ReasonCode != 0 {
		return nil, fmt.Errorf("MQTT connect refused with reason %d", connack.ReasonCode)
	}
	return client, nil
}

// Subscribe subscribes client to filter at qos. A refusal is reported with
// its reason code and the usual cause. paho returns the SUBACK together with
// an error when the broker refuses (paho/client.go, Subscribe, in paho.golang
// v0.23.0), so the SUBACK is read before the error.
func Subscribe(ctx context.Context, client *paho.Client, filter string, qos byte) error {
	suback, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}}})
	if suback != nil && len(suback.Reasons) == 1 && suback.Reasons[0] >= reasonCodeFailure {
		return fmt.Errorf("subscription to %s refused with reason 0x%02x; check the user's topic permissions", filter, suback.Reasons[0])
	}
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", filter, err)
	}
	return nil
}
