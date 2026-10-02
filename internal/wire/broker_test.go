package wire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// TestBrokerFiltersDeliverTheirOwnMessages publishes this package's messages
// through a real broker and checks that each consumer filter receives exactly
// its own: rx from two devices through the rx wildcard, a device event through
// the device status wildcard, a keepalive through the agent status wildcard.
// The topic tests prove the filters match on paper; this proves the broker and
// its topic permissions agree.
//
// It needs a broker, so it is skipped unless TEST_BROKER is set; make
// test-broker runs it against the development broker.
func TestBrokerFiltersDeliverTheirOwnMessages(t *testing.T) {
	addr := os.Getenv("TEST_BROKER")
	if addr == "" {
		t.Skip("TEST_BROKER is not set; make test-broker runs this against the development broker")
	}
	user, pass := os.Getenv("TEST_MQTT_USER"), os.Getenv("TEST_MQTT_PASS")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A run id in every device and instance keeps this run's messages apart
	// from any other traffic at the station, including another run's.
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("run id: %v", err)
	}
	run := hex.EncodeToString(random[:])
	station, err := NewStationTopics("acme", "vasby", "pack-03")
	if err != nil {
		t.Fatalf("station: %v", err)
	}
	instance := "wire-test-" + run
	agentTopics, err := station.Agent(instance)
	if err != nil {
		t.Fatalf("agent: %v", err)
	}
	devices := map[string]DeviceTopics{}
	for _, id := range []string{"wire-" + run + "-a", "wire-" + run + "-b"} {
		topics, err := station.Device(id)
		if err != nil {
			t.Fatalf("device %s: %v", id, err)
		}
		devices[id] = topics
	}

	filters := []string{station.EveryDeviceRx(), station.EveryDeviceStatus(), station.EveryAgentStatus()}
	received := map[string]chan *paho.Publish{}
	for index, filter := range filters {
		received[filter] = subscribe(ctx, t, addr, user, pass, fmt.Sprintf("wire-test-%s-sub-%d", run, index), filter, run)
	}

	builder := NewBuilder(Agent{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: instance, AgentVersion: "test"}, nil)
	now := time.Now()
	first, second := Device{ID: "wire-" + run + "-a", Expiry: 30 * time.Second}, Device{ID: "wire-" + run + "-b", Expiry: 30 * time.Second}
	sent := []struct {
		topic   string
		message any
	}{
		{devices[first.ID].Rx(), builder.Rx(first, 1, []byte("7310425012345"), now)},
		{devices[second.ID].Rx(), builder.Rx(second, 1, []byte("1.250 kg"), now)},
		{devices[first.ID].Status(), builder.PortOpened(first, "/dev/ttyACM0", now)},
		{agentTopics.Status(), builder.Keepalive(now, now, 15*time.Second, 3, []DeviceState{{Device: first}, {Device: second}})},
	}
	publisher := connect(ctx, t, addr, user, pass, "wire-test-"+run+"-pub")
	for _, item := range sent {
		body, err := json.Marshal(item.message)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		expiry := uint32(30)
		if _, err := publisher.Publish(ctx, &paho.Publish{
			Topic: item.topic, QoS: 1, Payload: body,
			Properties: &paho.PublishProperties{ContentType: "application/json", MessageExpiry: &expiry},
		}); err != nil {
			t.Fatalf("publish %s: %v", item.topic, err)
		}
	}

	want := map[string][]string{
		station.EveryDeviceRx():     {devices[first.ID].Rx() + " rx", devices[second.ID].Rx() + " rx"},
		station.EveryDeviceStatus(): {devices[first.ID].Status() + " event"},
		station.EveryAgentStatus():  {agentTopics.Status() + " keepalive"},
	}
	for _, filter := range filters {
		got := collect(t, received[filter], 3*time.Second)
		sort.Strings(got)
		sort.Strings(want[filter])
		if strings.Join(got, "\n") != strings.Join(want[filter], "\n") {
			t.Errorf("%s received\n  %s\nwant\n  %s", filter, strings.Join(got, "\n  "), strings.Join(want[filter], "\n  "))
		}
	}
}

// collect returns "<topic> <kind>" for every message that arrives before the
// channel has been quiet for quiet.
func collect(t *testing.T, messages chan *paho.Publish, quiet time.Duration) []string {
	t.Helper()
	var got []string
	for {
		select {
		case message := <-messages:
			var fields struct {
				Schema int    `json:"schema"`
				Kind   string `json:"kind"`
			}
			if err := json.Unmarshal(message.Payload, &fields); err != nil || fields.Schema != Schema {
				t.Errorf("%s: payload %s does not decode as schema %d: %v", message.Topic, message.Payload, Schema, err)
				continue
			}
			got = append(got, message.Topic+" "+fields.Kind)
		case <-time.After(quiet):
			return got
		}
	}
}

// subscribe returns this run's messages matching filter. Other traffic at the
// station is dropped in the handler, and so is anything beyond the buffer,
// because a handler that blocks stalls the client's read loop.
func subscribe(ctx context.Context, t *testing.T, addr, user, pass, clientID, filter, run string) chan *paho.Publish {
	t.Helper()
	messages := make(chan *paho.Publish, 32)
	client := connectWith(ctx, t, addr, user, pass, clientID,
		func(received paho.PublishReceived) (bool, error) {
			if strings.Contains(received.Packet.Topic, run) {
				select {
				case messages <- received.Packet:
				default:
				}
			}
			return true, nil
		})
	ack, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}})
	if err != nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	for _, code := range ack.Reasons {
		if code > 1 {
			t.Fatalf("subscribe %s: refused with reason %d", filter, code)
		}
	}
	return messages
}

func connect(ctx context.Context, t *testing.T, addr, user, pass, clientID string) *paho.Client {
	t.Helper()
	return connectWith(ctx, t, addr, user, pass, clientID, nil)
}

func connectWith(ctx context.Context, t *testing.T, addr, user, pass, clientID string, onPublish func(paho.PublishReceived) (bool, error)) *paho.Client {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	config := paho.ClientConfig{ClientID: clientID, Conn: conn}
	if onPublish != nil {
		config.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPublish}
	}
	client := paho.NewClient(config)
	packet := &paho.Connect{ClientID: clientID, KeepAlive: 30, CleanStart: true}
	if user != "" {
		packet.Username, packet.UsernameFlag = user, true
		packet.Password, packet.PasswordFlag = []byte(pass), true
	}
	ack, err := client.Connect(ctx, packet)
	if err != nil {
		t.Fatalf("connect %s as %s: %v", addr, clientID, err)
	}
	if ack.ReasonCode != 0 {
		t.Fatalf("connect %s as %s: reason %d", addr, clientID, ack.ReasonCode)
	}
	t.Cleanup(func() { _ = client.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return client
}

// TestBrokerPermissions publishes as each development user to the topics the
// permissions in dev/rabbitmq/definitions.json are meant to allow and refuse,
// and checks what the broker does with each: a sender may publish tx to a
// device, and nothing that would pass for a reading or a status; a station may
// not reach another station's devices.
//
// RabbitMQ refuses a publish by closing the connection, not with a reason code
// (measured on 4.3.5, T11), so a publish is judged by two things: whether the
// broker acknowledged it, and whether a subscriber allowed to see the topic
// received it.
//
// It needs a broker, so it is skipped unless TEST_BROKER is set; make
// test-broker runs it as station-pack-03 and ingest.
func TestBrokerPermissions(t *testing.T) {
	addr := os.Getenv("TEST_BROKER")
	if addr == "" {
		t.Skip("TEST_BROKER is not set; make test-broker runs this against the development broker")
	}
	station := user{os.Getenv("TEST_MQTT_USER"), os.Getenv("TEST_MQTT_PASS")}
	ingest := user{os.Getenv("TEST_INGEST_USER"), os.Getenv("TEST_INGEST_PASS")}
	if ingest.name == "" {
		t.Fatal("TEST_INGEST_USER is not set; make test-broker sets it to the development ingest user")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("run id: %v", err)
	}
	run := hex.EncodeToString(random[:])
	own, err := NewStationTopics("acme", "vasby", "pack-03")
	if err != nil {
		t.Fatalf("station: %v", err)
	}
	other, err := NewStationTopics("acme", "vasby", "pack-04")
	if err != nil {
		t.Fatalf("other station: %v", err)
	}
	device := "perm-" + run
	ownDevice, err := own.Device(device)
	if err != nil {
		t.Fatalf("device: %v", err)
	}
	otherDevice, err := other.Device(device)
	if err != nil {
		t.Fatalf("other station's device: %v", err)
	}

	checks := []struct {
		name      string
		publisher user
		topic     string
		observer  user
		allowed   bool
	}{
		{"a sender publishes tx to a device", ingest, ownDevice.Tx(), station, true},
		{"a sender cannot publish a reading", ingest, ownDevice.Rx(), station, false},
		{"a sender cannot publish a device status", ingest, ownDevice.Status(), station, false},
		{"a station cannot publish to another station's tx", station, otherDevice.Tx(), ingest, false},
	}
	for i, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			received := subscribe(ctx, t, addr, check.observer.name, check.observer.pass,
				fmt.Sprintf("perm-%s-observer-%d", run, i), check.topic, run)
			publishErr := publishOnce(ctx, t, addr, check.publisher, fmt.Sprintf("perm-%s-publisher-%d", run, i), check.topic)
			delivered := len(collect(t, received, 2*time.Second)) > 0
			switch {
			case check.allowed && (publishErr != nil || !delivered):
				t.Errorf("%s publishing to %s: error %v, delivered %v; want acknowledged and delivered",
					check.publisher.name, check.topic, publishErr, delivered)
			case !check.allowed && (publishErr == nil || delivered):
				t.Errorf("%s publishing to %s: error %v, delivered %v; want refused and not delivered",
					check.publisher.name, check.topic, publishErr, delivered)
			}
		})
	}
}

type user struct{ name, pass string }

// publishOnce connects as publisher and publishes one schema 2 message at QoS
// 1. It returns nil only when the broker acknowledged it with a success code.
func publishOnce(ctx context.Context, t *testing.T, addr string, publisher user, clientID, topic string) error {
	t.Helper()
	client := connect(ctx, t, addr, publisher.name, publisher.pass, clientID)
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ack, err := client.Publish(publishCtx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte(`{"schema":2,"kind":"permission-check"}`)})
	if err != nil {
		return err
	}
	if ack.ReasonCode >= 0x80 {
		return fmt.Errorf("refused with reason 0x%02x", ack.ReasonCode)
	}
	return nil
}
