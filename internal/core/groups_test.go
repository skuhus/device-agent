package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/logging/logtest"
	"github.com/skuhus/device-agent/internal/wire"
)

// groupRun is a core with three printers: scales-a in the site group scales
// and the station group front, scales-b in the site group scales, and
// printer-1 in no group.
type groupRun struct {
	t         *testing.T
	transport *fakeTransport
	printers  map[string]*fakePrinter
	devices   map[string]Device
	txIn      chan TxMessage
	connected chan struct{}
	log       *logtest.Log
}

func groupRoute(t *testing.T, scope wire.TxScope, group string) wire.TxRoute {
	t.Helper()
	route, err := station.GroupTx(scope, group)
	if err != nil {
		t.Fatalf("group %s at %s: %v", group, scope, err)
	}
	return route
}

func startGroupRun(t *testing.T) *groupRun {
	t.Helper()
	return startGroupRunAnswered(t, nil)
}

// startGroupRunAnswered starts the run with answers as the broker's answers to
// the subscriptions.
func startGroupRunAnswered(t *testing.T, answers map[string]byte) *groupRun {
	t.Helper()
	run := &groupRun{t: t, transport: &fakeTransport{answers: answers}, printers: map[string]*fakePrinter{}, devices: map[string]Device{},
		txIn: make(chan TxMessage, 16), connected: make(chan struct{}, 1)}
	siteScales, stationFront := groupRoute(t, wire.ScopeSite, "scales"), groupRoute(t, wire.ScopeStation, "front")
	var devices []Device
	for _, setup := range []struct {
		id     string
		groups []wire.TxRoute
	}{
		{"scales-a", []wire.TxRoute{siteScales, stationFront}},
		{"scales-b", []wire.TxRoute{siteScales}},
		{"printer-1", nil},
	} {
		printer := newFakePrinter(setup.id, true)
		dev := coreDevice(t, printer, "")
		dev.BroadcastGroups = setup.groups
		run.printers[setup.id], run.devices[setup.id] = printer, dev
		devices = append(devices, dev)
	}
	opts := testOptions(t, run.transport, devices...)
	logger, log := logtest.New(t, "debug")
	opts.Logger, opts.TxIn, opts.Connected, run.log = logger, run.txIn, run.connected, log
	running := newCore(t, opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- running.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return within 10s of cancellation")
		}
	})
	for id, printer := range run.printers {
		waitUntil(t, id+"'s port reported open", printer.announcedOpen)
	}
	return run
}

func (run *groupRun) send(topic, id string, data []byte) {
	payload, err := json.Marshal(map[string]any{"schema": 2, "id": id, "sender": "scale-poller", "raw_b64": base64.StdEncoding.EncodeToString(data)})
	if err != nil {
		run.t.Fatal(err)
	}
	run.txIn <- TxMessage{Topic: topic, Payload: payload, Expiry: 30 * time.Second, HasExpiry: true, Received: time.Now()}
}

// resultsOf is every tx result for id on the device's own status topic.
func (run *groupRun) resultsOf(deviceID, id string) []wire.TxResult {
	var mine []wire.TxResult
	for _, message := range run.transport.of("event", run.devices[deviceID].Topics.Status()) {
		var result wire.TxResult
		if err := json.Unmarshal(message.payload, &result); err != nil {
			run.t.Fatalf("status message is not JSON: %v", err)
		}
		if result.Kind == wire.KindTxResult && result.TxID != nil && *result.TxID == id {
			mine = append(mine, result)
		}
	}
	return mine
}

// A tx on a group's topic is taken by every device in the group, as if each had
// been sent it on its own topic: each writes it once and answers on its own
// status topic under the tx's id. A device outside the group gets nothing, and
// a group at another scope reaches only its own devices.
func TestGroupTxReachesEveryDeviceInTheGroup(t *testing.T) {
	run := startGroupRun(t)
	poll := []byte{0x05}
	run.send(groupRoute(t, wire.ScopeSite, "scales").Topic, txA, poll)
	for _, id := range []string{"scales-a", "scales-b"} {
		waitUntil(t, id+"'s written result", func() bool { return len(run.resultsOf(id, txA)) == 2 })
		if got := codes(run.resultsOf(id, txA)); got != "accepted/accepted written/written" {
			t.Errorf("%s results = %s, want accepted, then written", id, got)
		}
	}
	run.send(groupRoute(t, wire.ScopeStation, "front").Topic, txB, []byte("front"))
	waitUntil(t, "scales-a's written result for the station group", func() bool { return len(run.resultsOf("scales-a", txB)) == 2 })

	for id, want := range map[string][][]byte{
		"scales-a":  {poll, []byte("front")},
		"scales-b":  {poll},
		"printer-1": nil,
	} {
		writes, _ := run.printers[id].snapshot()
		if len(writes) != len(want) {
			t.Errorf("%s writes = %q, want %q", id, writes, want)
			continue
		}
		for index := range want {
			if !bytes.Equal(writes[index], want[index]) {
				t.Errorf("%s write %d = %q, want %q", id, index, writes[index], want[index])
			}
		}
	}
	for _, id := range []string{"scales-b", "printer-1"} {
		if results := run.resultsOf(id, txB); len(results) != 0 {
			t.Errorf("%s answered the station group's tx: %s", id, codes(results))
		}
	}
	if results := run.resultsOf("printer-1", txA); len(results) != 0 {
		t.Errorf("printer-1 answered the site group's tx: %s", codes(results))
	}

	fanned := run.log.WithMessage(t, "tx on a broadcast group's topic; each device in the group takes it")
	requireRecord(t, fanned, "topic", "skuhus/acme/vasby/group/scales/tx", "scope", "site", "group", "scales")
	if len(fanned) != 2 || len(fanned[0]["device_ids"].([]any)) != 2 || fanned[0]["device_ids"].([]any)[0] != "scales-a" || fanned[0]["device_ids"].([]any)[1] != "scales-b" {
		t.Errorf("fan-out records = %v, want two, the first naming scales-a and scales-b", fanned)
	}
	requireRecord(t, run.log.WithMessage(t, "tx accepted"), "tx_id", txA, "device_id", "scales-b", "topic", "skuhus/acme/vasby/group/scales/tx")
}

// The same tx sent again to the group is answered by each device for itself,
// already_written, and written by none of them again: the ids written are each
// device's own (#11 Q6a).
func TestGroupTxResendIsAnsweredByEachDevice(t *testing.T) {
	run := startGroupRun(t)
	site := groupRoute(t, wire.ScopeSite, "scales").Topic
	run.send(site, txA, []byte{0x05})
	for _, id := range []string{"scales-a", "scales-b"} {
		waitUntil(t, id+"'s written result", func() bool { return len(run.resultsOf(id, txA)) == 2 })
	}
	run.send(site, txA, []byte{0x05})
	for _, id := range []string{"scales-a", "scales-b"} {
		waitUntil(t, id+"'s answer to the resend", func() bool { return len(run.resultsOf(id, txA)) == 3 })
		if got := codes(run.resultsOf(id, txA)); got != "accepted/accepted written/written written/already_written" {
			t.Errorf("%s results = %s, want the resend answered already_written", id, got)
		}
		if writes, _ := run.printers[id].snapshot(); len(writes) != 1 {
			t.Errorf("%s was written %d times, want once", id, len(writes))
		}
	}
}

// The keepalive lists, for each device, every topic that reaches its tx, its
// own first, each with the broker's answer under the topic as subscribed: a
// granted QoS, a refusal, or null where the broker has not answered.
func TestKeepaliveListsEveryTxTopicWithItsAnswer(t *testing.T) {
	ownA, _ := station.Device("scales-a")
	ownB, _ := station.Device("scales-b")
	site, front := groupRoute(t, wire.ScopeSite, "scales"), groupRoute(t, wire.ScopeStation, "front")
	run := startGroupRunAnswered(t, map[string]byte{ownA.Tx(): 1, ownB.Tx(): 0x87, site.Topic: 1})
	run.connected <- struct{}{}
	waitUntil(t, "a keepalive", func() bool { return len(run.transport.keepalives()) > 0 })

	answer := func(code int) *int { return &code }
	group := func(name string) *string { return &name }
	printerOwn, _ := station.Device("printer-1")
	want := map[string][]wire.TxTopic{
		"scales-a": {
			{Topic: ownA.Tx(), Scope: wire.ScopeDevice, Suback: answer(1)},
			{Topic: site.Topic, Scope: wire.ScopeSite, Group: group("scales"), Suback: answer(1)},
			{Topic: front.Topic, Scope: wire.ScopeStation, Group: group("front")},
		},
		"scales-b": {
			{Topic: ownB.Tx(), Scope: wire.ScopeDevice, Suback: answer(0x87)},
			{Topic: site.Topic, Scope: wire.ScopeSite, Group: group("scales"), Suback: answer(1)},
		},
		"printer-1": {{Topic: printerOwn.Tx(), Scope: wire.ScopeDevice}},
	}
	devices := run.transport.keepalives()[0].Devices
	if len(devices) != len(want) {
		t.Fatalf("%d devices in the keepalive, want %d", len(devices), len(want))
	}
	for _, device := range devices {
		got, _ := json.Marshal(device.TxTopics)
		expected, _ := json.Marshal(want[device.DeviceID])
		if string(got) != string(expected) {
			t.Errorf("%s tx_topics = %s\nwant %s", device.DeviceID, got, expected)
		}
	}
}
