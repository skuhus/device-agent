// Command consumer subscribes to a station's topics and prints what arrives.
//
// It is the other end of the wire during development: the agent publishes, this
// prints, and the two together say whether a scan made it out of the building.
// It is not part of the agent and is not built by "make build".
//
// Scan envelopes get one extra line beyond the JSON, decoding raw_b64 back to
// bytes, because that field is the whole point of the exercise and base64 is
// not readable at a glance.
package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/eclipse/paho.golang/paho"
)

func main() {
	fs := flag.NewFlagSet("consumer", flag.ContinueOnError)
	broker := fs.String("broker", "skuhus-dev-rabbitmq:1883", "broker address as host:port")
	username := fs.String("username", "ingest", "broker username")
	password := fs.String("password", "ingest-dev", "broker password")
	topic := fs.String("topic", "skuhus/#", "topic filter to subscribe to")
	qos := fs.Uint("qos", 1, "subscription QoS")
	clientID := fs.String("client-id", "", "client id; defaults to consumer-<pid>")
	raw := fs.Bool("raw", false, "print payloads exactly as received, without formatting JSON")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if *clientID == "" {
		*clientID = fmt.Sprintf("consumer-%d", os.Getpid())
	}

	if err := run(*broker, *username, *password, *topic, *clientID, byte(*qos), *raw); err != nil {
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
}

func run(broker, username, password, topic, clientID string, qos byte, raw bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := net.DialTimeout("tcp", broker, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", broker, err)
	}

	mqttClient := paho.NewClient(paho.ClientConfig{
		ClientID: clientID,
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				print(pr.Packet, raw)
				return true, nil
			},
		},
		// A dropped connection ends the run rather than being retried: this is a
		// diagnostic tool, and silently reconnecting would hide the very
		// disconnect someone is watching for.
		OnClientError: func(err error) { fmt.Fprintln(os.Stderr, "connection error: "+err.Error()) },
		OnServerDisconnect: func(d *paho.Disconnect) {
			fmt.Fprintf(os.Stderr, "server disconnected, reason %d\n", d.ReasonCode)
			stop()
		},
	})

	cp := &paho.Connect{
		ClientID:   clientID,
		KeepAlive:  30,
		CleanStart: true,
	}
	if username != "" {
		cp.Username, cp.UsernameFlag = username, true
		cp.Password, cp.PasswordFlag = []byte(password), true
	}
	ca, err := mqttClient.Connect(ctx, cp)
	if err != nil {
		return fmt.Errorf("MQTT connect: %w", err)
	}
	if ca.ReasonCode != 0 {
		return fmt.Errorf("MQTT connect refused with reason %d", ca.ReasonCode)
	}

	sa, err := mqttClient.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: qos}},
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", topic, err)
	}
	for _, code := range sa.Reasons {
		if code > 2 {
			return fmt.Errorf("subscription to %s refused with reason %d; check the user's topic permissions", topic, code)
		}
	}

	fmt.Printf("subscribed to %s on %s as %s, waiting\n\n", topic, broker, username)
	<-ctx.Done()
	fmt.Fprintln(os.Stderr, "\nstopping")
	_ = mqttClient.Disconnect(&paho.Disconnect{ReasonCode: 0})
	return nil
}

// print writes one message: a header line that is greppable, the payload, and
// for a scan envelope the decoded payload bytes.
func print(packet *paho.Publish, raw bool) {
	header := fmt.Sprintf("%s  %s  qos=%d", time.Now().UTC().Format("15:04:05.000"), packet.Topic, packet.QoS)
	if packet.Retain {
		header += " retained"
	}
	if props := describeProperties(packet); props != "" {
		header += "  " + props
	}
	fmt.Println(header)

	if raw {
		fmt.Printf("%s\n\n", packet.Payload)
		return
	}
	if len(packet.Payload) == 0 {
		fmt.Print("  (empty payload: a retained message being cleared)\n\n")
		return
	}

	var envelope map[string]any
	if err := json.Unmarshal(packet.Payload, &envelope); err != nil {
		fmt.Printf("  not JSON: %s\n\n", preview(packet.Payload))
		return
	}
	indented, err := json.MarshalIndent(envelope, "  ", "  ")
	if err != nil {
		fmt.Printf("  %s\n\n", packet.Payload)
		return
	}
	fmt.Printf("  %s\n", indented)
	if decoded := decodeRaw(envelope); decoded != "" {
		fmt.Printf("  payload: %s\n", decoded)
	}
	fmt.Println()
}

// describeProperties surfaces the MQTT 5 properties that carry meaning for this
// agent: the command channel's correlation, and the expiry a scan was published
// with.
func describeProperties(packet *paho.Publish) string {
	if packet.Properties == nil {
		return ""
	}
	var parts []string
	if packet.Properties.MessageExpiry != nil {
		parts = append(parts, fmt.Sprintf("expiry=%ds", *packet.Properties.MessageExpiry))
	}
	if packet.Properties.ResponseTopic != "" {
		parts = append(parts, "response_topic="+packet.Properties.ResponseTopic)
	}
	if len(packet.Properties.CorrelationData) > 0 {
		parts = append(parts, "correlation="+preview(packet.Properties.CorrelationData))
	}
	return strings.Join(parts, " ")
}

// decodeRaw turns the envelope's raw_b64 back into bytes. A scan payload can
// carry 0x1D group separators and a vendor code identifier, neither of which
// survives being read as a quoted string, so non-printable bytes are shown as
// hex alongside the text.
func decodeRaw(envelope map[string]any) string {
	encoded, ok := envelope["raw_b64"].(string)
	if !ok {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "raw_b64 is not valid base64: " + err.Error()
	}
	return fmt.Sprintf("%s  (%d bytes, hex %s)", preview(decoded), len(decoded), hex.EncodeToString(decoded))
}

// preview renders bytes as text when they are printable UTF-8, and as hex when
// they are not, so a control character is never silently swallowed by a
// terminal.
func preview(raw []byte) string {
	if !utf8.Valid(raw) {
		return "hex:" + hex.EncodeToString(raw)
	}
	for _, char := range string(raw) {
		if char < 0x20 || char == 0x7f {
			return fmt.Sprintf("%q", string(raw))
		}
	}
	return fmt.Sprintf("%q", string(raw))
}
