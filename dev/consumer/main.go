// Command consumer subscribes to a station's topics and prints what arrives.
//
// It is the other end of the wire during development: the agent publishes, this
// prints, and the two together say whether a scan made it out of the building.
// It is not part of the agent and is not built by "make build".
//
// A message that carries raw_b64, an rx or a tx, gets one extra line beyond the
// JSON, decoding it back to bytes, because that field is the whole point of the
// exercise and base64 is not readable at a glance.
package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/eclipse/paho.golang/paho"
	"github.com/skuhus/device-agent/dev/internal/mqttconn"
)

func main() {
	flags := flag.NewFlagSet("consumer", flag.ContinueOnError)
	broker := flags.String("broker", "skuhus-dev-rabbitmq:1883", "broker address as host:port")
	username := flags.String("username", "ingest", "broker username")
	password := flags.String("password", "ingest-dev", "broker password")
	topic := flags.String("topic", "skuhus/#", "topic filter to subscribe to")
	qos := flags.Uint("qos", 1, "subscription QoS")
	clientID := flags.String("client-id", "", "client id; defaults to consumer-<pid>")
	raw := flags.Bool("raw", false, "print payloads exactly as received, without formatting JSON")

	if err := flags.Parse(os.Args[1:]); err != nil {
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

	// A dropped connection ends the run with its error rather than being
	// retried: this is a diagnostic tool, and reconnecting, or carrying on
	// subscribed to nothing, would hide the very disconnect someone is watching
	// for. paho reports a broken connection through OnClientError and a
	// broker's DISCONNECT through OnServerDisconnect, so both end it (#24).
	lost := make(chan error, 1)
	end := func(err error) {
		select {
		case lost <- err:
		default:
		}
		stop()
	}
	mqttClient, err := mqttconn.Connect(ctx, broker, username, password, paho.ClientConfig{
		ClientID: clientID,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(received paho.PublishReceived) (bool, error) {
				printMessage(received.Packet, raw)
				return true, nil
			},
		},
		OnClientError: func(err error) { end(fmt.Errorf("connection lost: %w", err)) },
		OnServerDisconnect: func(disconnect *paho.Disconnect) {
			end(fmt.Errorf("the broker disconnected, reason %d", disconnect.ReasonCode))
		},
	})
	if err != nil {
		return err
	}
	if err := mqttconn.Subscribe(ctx, mqttClient, topic, qos); err != nil {
		return err
	}

	fmt.Printf("subscribed to %s on %s as %s, waiting\n\n", topic, broker, username)
	<-ctx.Done()
	select {
	case err := <-lost:
		return err
	default:
	}
	fmt.Fprintln(os.Stderr, "\nstopping")
	_ = mqttClient.Disconnect(&paho.Disconnect{ReasonCode: 0})
	return nil
}

// printMessage writes one message: a header line that is greppable, the
// payload, and for an rx or a tx the decoded bytes.
func printMessage(packet *paho.Publish, raw bool) {
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
		fmt.Print("  (empty payload)\n\n")
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

// describeProperties surfaces the MQTT 5 properties worth seeing: the message
// expiry each message was published with, and a response topic or correlation
// data, which the agent does not use (#23 Q4) but a sender may set.
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

// decodeRaw turns a message's raw_b64 back into bytes. A frame can carry 0x1D
// group separators and a vendor code identifier, neither of which survives
// being read as a quoted string, so the bytes are shown as hex alongside the
// text.
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

// preview renders bytes as a quoted string when they are valid UTF-8, which
// escapes every control character, and as hex when they are not, so a control
// character is never silently swallowed by a terminal.
func preview(raw []byte) string {
	if !utf8.Valid(raw) {
		return "hex:" + hex.EncodeToString(raw)
	}
	return fmt.Sprintf("%q", string(raw))
}
