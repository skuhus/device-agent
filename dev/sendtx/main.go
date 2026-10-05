// Command sendtx publishes a tx to a device, as a sender does, and prints the
// results the agent publishes for it.
//
// It is the writing counterpart of the consumer: run it as the ingest user,
// which may write device tx topics and read everything. It is not part of the
// agent and is not built by "make build".
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func main() {
	fs := flag.NewFlagSet("sendtx", flag.ContinueOnError)
	broker := fs.String("broker", "skuhus-dev-rabbitmq:1883", "broker address as host:port")
	username := fs.String("username", "ingest", "broker username")
	password := fs.String("password", "ingest-dev", "broker password")
	topic := fs.String("topic", "", "the device's tx topic, skuhus/<project>/<site>/<station>/<device>/tx")
	text := fs.String("text", "", `bytes to write, with \r, \n, \t, \\ and \xNN decoded`)
	file := fs.String("file", "", "write the bytes of this file instead")
	hexData := fs.String("hex", "", "write these bytes, given as hex, instead")
	id := fs.String("id", "", "the tx id; a new UUID by default. Reuse one to see in_progress or a resend")
	sender := fs.String("sender", "make-send-tx", "the tx's sender")
	expiry := fs.Duration("expiry", 30*time.Second, "the tx's MQTT message expiry; 0 sends none")
	wait := fs.Duration("wait", 15*time.Second, "how long to wait for the tx's final result")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	data, err := payload(*text, *file, *hexData)
	if err == nil && !strings.HasSuffix(*topic, "/tx") {
		err = fmt.Errorf("--topic %q is not a device's tx topic", *topic)
	}
	if err == nil && *id == "" {
		*id, err = newUUID()
	}
	if err == nil {
		err = run(*broker, *username, *password, *topic, *id, *sender, data, *expiry, *wait)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
}

func payload(text, file, hexData string) ([]byte, error) {
	given := 0
	for _, value := range []string{text, file, hexData} {
		if value != "" {
			given++
		}
	}
	if given != 1 {
		return nil, errors.New("give exactly one of --text, --file and --hex")
	}
	switch {
	case file != "":
		return os.ReadFile(file)
	case hexData != "":
		return hex.DecodeString(strings.ReplaceAll(hexData, " ", ""))
	}
	return unescape(text)
}

// unescape decodes the escapes a shell makes awkward to type as bytes.
func unescape(text string) ([]byte, error) {
	var out []byte
	for index := 0; index < len(text); index++ {
		if text[index] != '\\' || index+1 == len(text) {
			out = append(out, text[index])
			continue
		}
		index++
		switch text[index] {
		case 'r':
			out = append(out, '\r')
		case 'n':
			out = append(out, '\n')
		case 't':
			out = append(out, '\t')
		case '\\':
			out = append(out, '\\')
		case 'x':
			if index+2 >= len(text) {
				return nil, fmt.Errorf("\\x at %d needs two hex digits", index-1)
			}
			value, err := strconv.ParseUint(text[index+1:index+3], 16, 8)
			if err != nil {
				return nil, fmt.Errorf("\\x%s: %w", text[index+1:index+3], err)
			}
			out = append(out, byte(value))
			index += 2
		default:
			return nil, fmt.Errorf("unknown escape \\%c", text[index])
		}
	}
	return out, nil
}

// newUUID makes a version 4 UUID, which every agent accepts (DESIGN-V2.md, "tx").
func newUUID() (string, error) {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "", err
	}
	randomBytes[6] = randomBytes[6]&0x0f | 0x40
	randomBytes[8] = randomBytes[8]&0x3f | 0x80
	hexDigits := hex.EncodeToString(randomBytes[:])
	return hexDigits[0:8] + "-" + hexDigits[8:12] + "-" + hexDigits[12:16] + "-" + hexDigits[16:20] + "-" + hexDigits[20:32], nil
}

func run(broker, username, password, topic, id, sender string, data []byte, expiry, wait time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait+10*time.Second)
	defer cancel()
	conn, err := net.DialTimeout("tcp", broker, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", broker, err)
	}
	results := make(chan map[string]any, 16)
	// Named after the tx, not the process: in a container every process is
	// pid 1, and two senders sharing a client id disconnect each other.
	clientID := "sendtx-" + id
	client := paho.NewClient(paho.ClientConfig{
		ClientID: clientID,
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(received paho.PublishReceived) (bool, error) {
				var message map[string]any
				if json.Unmarshal(received.Packet.Payload, &message) == nil && message["kind"] == "tx_result" && message["tx_id"] == id {
					results <- message
				}
				return true, nil
			},
		},
	})
	connack, err := client.Connect(ctx, &paho.Connect{
		ClientID: clientID, KeepAlive: 30, CleanStart: true,
		Username: username, UsernameFlag: true, Password: []byte(password), PasswordFlag: true,
	})
	if err != nil {
		return fmt.Errorf("MQTT connect: %w", err)
	}
	if connack.ReasonCode != 0 {
		return fmt.Errorf("MQTT connect refused with reason %d", connack.ReasonCode)
	}
	defer client.Disconnect(&paho.Disconnect{ReasonCode: 0})

	// Subscribed before the tx goes out, so that no result is missed.
	status := strings.TrimSuffix(topic, "/tx") + "/status"
	suback, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: status, QoS: 1}}})
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", status, err)
	}
	if len(suback.Reasons) != 1 || suback.Reasons[0] > 2 {
		return fmt.Errorf("subscription to %s refused: %v", status, suback.Reasons)
	}

	body, err := json.Marshal(map[string]any{"schema": 2, "id": id, "sender": sender, "raw_b64": base64.StdEncoding.EncodeToString(data)})
	if err != nil {
		return err
	}
	properties := &paho.PublishProperties{ContentType: "application/json"}
	if expiry > 0 {
		seconds := uint32((expiry + time.Second - 1) / time.Second)
		properties.MessageExpiry = &seconds
	}
	puback, err := client.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: body, Properties: properties})
	if err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	if puback != nil && puback.ReasonCode >= 0x80 {
		return fmt.Errorf("publish to %s refused with reason 0x%02x", topic, puback.ReasonCode)
	}
	sent := time.Now()
	fmt.Printf("sent tx %s, %d bytes, to %s at %s\n", id, len(data), topic, sent.UTC().Format("15:04:05.000"))

	deadline := time.After(wait)
	for {
		select {
		case result := <-results:
			detail, _ := json.Marshal(result["detail"])
			fmt.Printf("+%s  %s  %s  %s  %s\n", time.Since(sent).Round(time.Millisecond), result["state"], result["code"], result["text"], detail)
			switch result["state"] {
			case "written", "failed", "rejected":
				return nil
			}
		case <-deadline:
			return fmt.Errorf("no final result within %s", wait)
		}
	}
}
