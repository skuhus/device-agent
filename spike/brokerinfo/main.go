// Command brokerinfo identifies a broker before the property spike runs against
// it. It answers two questions that decide whether the M2 design is even
// applicable: which MQTT protocol levels the broker accepts, and, over AMQP,
// which product and version it is.
//
// It exists because a broker that does not speak MQTT 5 closes the connection
// without a CONNACK, and a client library reports that as "EOF", which is
// indistinguishable from a network problem. Sending the CONNECT by hand at each
// protocol level turns that into an answer.
//
// Both probes are read-only: the MQTT probe sends a CONNECT with no credentials
// and never publishes or subscribes, and the AMQP probe stops at the protocol
// header, before authentication.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"time"
)

func main() {
	fs := flag.NewFlagSet("brokerinfo", flag.ContinueOnError)
	mqttAddr := fs.String("mqtt", "", "MQTT address as host:port")
	amqpAddr := fs.String("amqp", "", "AMQP 0-9-1 address as host:port, which reports the product and version")
	clientID := fs.String("client-id", "brokerinfo-probe", "client id sent in the CONNECT")
	timeout := fs.Duration("timeout", 8*time.Second, "dial and read timeout")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if *mqttAddr == "" && *amqpAddr == "" {
		fmt.Fprintln(os.Stderr, "error: pass --mqtt, --amqp, or both")
		os.Exit(2)
	}

	if *mqttAddr != "" {
		for _, level := range []byte{4, 5} {
			fmt.Println(probeMQTT(*mqttAddr, level, *clientID, *timeout))
		}
	}
	if *amqpAddr != "" {
		fmt.Println(probeAMQP(*amqpAddr, *timeout))
	}
}

var levelNames = map[byte]string{4: "3.1.1", 5: "5.0"}

// probeMQTT sends one CONNECT and reports the raw reply. A CONNACK means the
// level is supported enough to be answered, whatever the reason code says about
// credentials; a closed connection means the broker would not talk at all.
func probeMQTT(addr string, level byte, clientID string, timeout time.Duration) string {
	name := fmt.Sprintf("mqtt %-5s", levelNames[level])
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Sprintf("%s dial failed: %v", name, err)
	}
	defer conn.Close()

	if _, err := conn.Write(connectPacket(level, clientID)); err != nil {
		return fmt.Sprintf("%s write failed: %v", name, err)
	}
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 64)
	count, err := conn.Read(buf)
	if count == 0 {
		return fmt.Sprintf("%s no CONNACK, connection closed (%v): this protocol level is not supported", name, err)
	}
	reply := buf[:count]
	if reply[0] != 0x20 {
		return fmt.Sprintf("%s reply was not a CONNACK: %s", name, hex.EncodeToString(reply))
	}
	// CONNACK is fixed header, session present flag, then the reason code.
	reason := byte(0xff)
	if count >= 4 {
		reason = reply[3]
	}
	return fmt.Sprintf("%s CONNACK reason 0x%02x (%s), raw %s", name, reason, connackReason(level, reason), hex.EncodeToString(reply))
}

// connackReason names the codes a credential-free probe actually provokes.
// The two protocol levels number them differently.
func connackReason(level, code byte) string {
	if level == 4 {
		switch code {
		case 0x00:
			return "accepted; this broker takes unauthenticated clients"
		case 0x01:
			return "unacceptable protocol version"
		case 0x04:
			return "bad user name or password"
		case 0x05:
			return "not authorised"
		}
		return "see MQTT 3.1.1 table 3.1"
	}
	switch code {
	case 0x00:
		return "accepted; this broker takes unauthenticated clients"
	case 0x84:
		return "unsupported protocol version"
	case 0x86:
		return "bad user name or password"
	case 0x87:
		return "not authorised"
	}
	return "see MQTT 5 section 3.2.2.2"
}

// connectPacket builds a CONNECT with clean start, no will and no credentials.
func connectPacket(level byte, clientID string) []byte {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', level, 0x02, 0x00, 0x3c}
	if level == 5 {
		body = append(body, 0x00) // zero length property block
	}
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, clientID...)

	pkt := []byte{0x10}
	remaining := len(body)
	for {
		lengthDigit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			lengthDigit |= 0x80
		}
		pkt = append(pkt, lengthDigit)
		if remaining == 0 {
			break
		}
	}
	return append(pkt, body...)
}

var printable = regexp.MustCompile(`[ -~]{4,}`)

// probeAMQP reads the server properties out of Connection.Start, which RabbitMQ
// fills with its product, version, platform and cluster name. The reply is
// decoded as printable runs rather than as a field table: this is a
// identification probe, and a field table decoder would be more code than the
// question deserves.
func probeAMQP(addr string, timeout time.Duration) string {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Sprintf("amqp        dial failed: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		return fmt.Sprintf("amqp        write failed: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	count, err := conn.Read(buf)
	if count == 0 {
		return fmt.Sprintf("amqp        no reply: %v", err)
	}

	fields := map[string]string{}
	runs := printable.FindAllString(string(buf[:count]), -1)
	for index, run := range runs {
		for _, key := range []string{"product", "version", "platform", "cluster_name"} {
			// The value follows its key in the table, prefixed by a type byte
			// and a length that the printable run has already trimmed away.
			if run == key+"S" && index+1 < len(runs) {
				fields[key] = trimLeadingLength(runs[index+1])
			}
		}
	}
	if len(fields) == 0 {
		return "amqp        replied, but no server properties were recognisable"
	}
	return fmt.Sprintf("amqp        product %s version %s platform %s cluster %s",
		orUnknown(fields["product"]), orUnknown(fields["version"]),
		orUnknown(fields["platform"]), orUnknown(fields["cluster_name"]))
}

// trimLeadingLength drops the long-string length byte that survives into the
// printable run when its value happens to be a printable character. The test is
// the length itself rather than "does this look like a length": a version
// string starting "3" would otherwise lose its first digit.
func trimLeadingLength(text string) string {
	if len(text) > 1 && int(text[0]) == len(text)-1 {
		return text[1:]
	}
	return text
}

func orUnknown(text string) string {
	if text == "" {
		return "unknown"
	}
	return text
}
