//go:build integration && linux

// Package integration runs the agent's binary the way a station runs it,
// against a pseudo-terminal device and a real broker, and checks what reaches
// the broker and the log (PLAN-V2.md, T10).
//
// It needs a RabbitMQ broker started from dev/rabbitmq: TEST_BROKER names it,
// TEST_MQTT_USER and TEST_MQTT_PASS are the station's credentials, which the
// agent runs with, and TEST_INGEST_USER and TEST_INGEST_PASS the subscriber's.
// make test-integration sets them for the development broker.
//
// The agent reaches the broker through a relay in this process, so that a test
// can take the broker away mid-run without stopping a broker anyone else uses.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"golang.org/x/sys/unix"
)

// agentBinary is the agent, built once for every test with the race detector,
// so that a race in a real run fails the test that ran into it.
var agentBinary string

// capturePath is the real Symbol 05e0:1701 capture: six scans, each ending in
// CRLF.
const capturePath = "../../internal/device/serial/testdata/symbol-05e0-1701-crlf.bin"

func TestMain(suite *testing.M) {
	for _, name := range []string{"TEST_BROKER", "TEST_MQTT_USER", "TEST_MQTT_PASS", "TEST_INGEST_USER", "TEST_INGEST_PASS"} {
		if os.Getenv(name) == "" {
			fmt.Fprintf(os.Stderr, "%s is not set; make test-integration sets it for the development broker\n", name)
			os.Exit(1)
		}
	}
	dir, err := os.MkdirTemp("", "skuhus-integration-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temp dir:", err)
		os.Exit(1)
	}
	agentBinary = filepath.Join(dir, "skuhus-device-agent")
	build := exec.Command("go", "build", "-race", "-o", agentBinary, "github.com/skuhus/device-agent/cmd/skuhus-device-agent")
	build.Env = append(os.Environ(), "CGO_ENABLED=1")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build the agent: %v\n%s", err, out)
		os.Exit(1)
	}
	code := suite.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// Every frame is published once on its device's rx topic, byte-exact, with
// the device's expiry; the port's events and a keepalive with the device's
// counters arrive; and SIGTERM ends the run with the offline message and no
// will, the v1 defect that was found only by hand.
func TestReadingsEventsKeepaliveAndCleanStop(t *testing.T) {
	run := newRun(t)
	scans := captureScans(t)
	agent := run.start(t)

	run.waitFor(t, "port_opened", func(received message) bool { return received.isEvent(run.device, "port_opened") })
	writeAll(t, run.master, scans)

	rx := run.waitForCount(t, "six readings", 6, func(received message) bool { return received.topic == run.deviceTopic("rx") })
	for index, received := range rx {
		raw, err := base64.StdEncoding.DecodeString(received.str("raw_b64"))
		want := strings.TrimSuffix(string(scans[index]), "\r\n")
		if err != nil || string(raw) != want || received.num("seq") != float64(index+1) || received.str("device_type") != "symbol-05e0-1701" {
			t.Errorf("rx %d = %q seq %v type %q, want %q seq %d, symbol-05e0-1701", index, raw, received.num("seq"), received.str("device_type"), want, index+1)
		}
		if received.qos != 1 || received.expiry == nil || *received.expiry < 25 || *received.expiry > 30 {
			t.Errorf("rx %d arrived at QoS %d with expiry %s, want QoS 1 and the device's 30 s, less transit", index, received.qos, received.expiryText())
		}
	}

	captured := 0
	for _, scan := range scans {
		captured += len(scan)
	}
	keepalive := run.waitFor(t, "a keepalive with every reading counted", func(received message) bool {
		device, ok := received.firstDevice()
		return received.str("kind") == "keepalive" && ok && device["rx_frames"] == float64(6)
	})
	device, _ := keepalive.firstDevice()
	if device["device_id"] != run.device || device["rx_bytes"] != float64(captured) || device["device_open"] != true ||
		keepalive.num("interval_s") != 1 || keepalive.num("gone_after_s") != 3 {
		t.Errorf("keepalive = %v, device %v; want %s open, %d bytes read, interval 1 s, gone after 3 s", keepalive.body, device, run.device, captured)
	}

	run.master.Close()
	lost := run.waitFor(t, "port_lost", func(received message) bool { return received.isEvent(run.device, "port_lost") })
	if detail, _ := lost.body["detail"].(map[string]any); detail["error_class"] != "disconnected" || lost.body["device_open"] != false {
		t.Errorf("port_lost = %v, want class disconnected and the port closed", lost.body)
	}

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 15*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM, want 0", code)
	}
	offline := run.waitFor(t, "the offline message", func(received message) bool {
		return received.topic == run.agentTopic() && received.str("kind") == "offline"
	})
	if offline.str("reason") != "shutdown" || offline.qos != 1 {
		t.Errorf("offline = %v at QoS %d, want reason shutdown at QoS 1", offline.body, offline.qos)
	}
	time.Sleep(2 * time.Second)
	if wills := run.matching(func(received message) bool {
		return received.topic == run.agentTopic() && received.str("reason") == "will"
	}); len(wills) > 0 {
		t.Errorf("the broker published the will after a clean stop: %v", wills[0].body)
	}
	if published := agent.records(t, "rx published"); len(published) != 6 {
		t.Errorf("%d published records in the log, want 6", len(published))
	}
}

// A tx published through the broker, as a sender does, reaches the device's
// port byte for byte, and its sender hears that it was accepted and written.
// Sent again, it is answered already_written and not written twice.
func TestTxReachesThePort(t *testing.T) {
	run := newRun(t)
	agent := run.start(t)
	run.waitFor(t, "port_opened", func(received message) bool { return received.isEvent(run.device, "port_opened") })
	run.waitFor(t, "the agent to subscribe", func(received message) bool { return received.str("kind") == "keepalive" })
	// Subscribing happens just after the connection comes up, and a keepalive
	// goes out at the same moment; the record of the subscription is what says
	// a tx can be sent.
	waitUntil(t, "the tx subscription", 10*time.Second, func() bool { return len(agent.records(t, "subscribed")) > 0 })

	job := append([]byte("\x1b@e2e "+run.id+"\n"), bytes.Repeat([]byte("0123456789abcdef"), 192)...)
	const id = "0192a3b4-c5d6-4e8f-9a0b-1c2d3e4f5a6b"
	payload, _ := json.Marshal(map[string]any{"schema": 2, "id": id, "sender": "e2e", "raw_b64": base64.StdEncoding.EncodeToString(job)})
	expiry := uint32(30)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := run.observer.Publish(ctx, &paho.Publish{Topic: run.deviceTopic("tx"), QoS: 1, Payload: payload,
		Properties: &paho.PublishProperties{MessageExpiry: &expiry}}); err != nil {
		t.Fatalf("publish the tx: %v", err)
	}

	got := readExactly(t, run.master, len(job), 10*time.Second)
	if !bytes.Equal(got, job) {
		t.Errorf("the port got %d bytes that differ from the %d sent", len(got), len(job))
	}
	results := run.waitForCount(t, "accepted and written", 2, func(received message) bool { return received.str("kind") == "tx_result" && received.str("tx_id") == id })
	if results[0].str("state") != "accepted" || results[1].str("state") != "written" {
		t.Errorf("results = %s, %s; want accepted, then written", results[0].str("state"), results[1].str("state"))
	}
	detail, _ := results[1].body["detail"].(map[string]any)
	if detail["bytes_written"] != float64(len(job)) || results[1].qos != 1 || results[1].str("sender") != "e2e" {
		t.Errorf("written = %v at QoS %d, want %d bytes from e2e at QoS 1", results[1].body, results[1].qos, len(job))
	}
	// The same tx again, as a sender that timed out would resend it: it is
	// answered and not written a second time (#11 Q6a).
	if _, err := run.observer.Publish(ctx, &paho.Publish{Topic: run.deviceTopic("tx"), QoS: 1, Payload: payload,
		Properties: &paho.PublishProperties{MessageExpiry: &expiry}}); err != nil {
		t.Fatalf("publish the tx again: %v", err)
	}
	all := run.waitForCount(t, "the resend's answer", 3, func(received message) bool { return received.str("kind") == "tx_result" && received.str("tx_id") == id })
	if all[2].str("state") != "written" || all[2].str("code") != "already_written" {
		t.Errorf("the resend got %s/%s, want written/already_written", all[2].str("state"), all[2].str("code"))
	}
	if more := readFor(t, run.master, time.Second); len(more) > 0 {
		t.Errorf("the resend put %d more bytes on the port", len(more))
	}
	run.waitFor(t, "a keepalive counting one write", func(received message) bool {
		device, ok := received.firstDevice()
		return received.str("kind") == "keepalive" && ok && device["tx_written"] == float64(1)
	})

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 15*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM, want 0", code)
	}
	if written := agent.records(t, "tx written"); len(written) != 1 {
		t.Errorf("%d tx written records in the log, want 1", len(written))
	}
}

// readFor reads whatever the pseudo-terminal's master gets within d.
func readFor(t *testing.T, master *os.File, wait time.Duration) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 4096)
	if err := master.SetReadDeadline(time.Now().Add(wait)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	for {
		read, err := master.Read(buf)
		got = append(got, buf[:read]...)
		if err != nil {
			return got
		}
	}
}

// readExactly reads n bytes from the pseudo-terminal's master, or fails once
// timeout has passed.
func readExactly(t *testing.T, master *os.File, want int, timeout time.Duration) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 4096)
	deadline := time.Now().Add(timeout)
	for len(got) < want {
		if err := master.SetReadDeadline(deadline); err != nil {
			t.Fatalf("read deadline: %v", err)
		}
		read, err := master.Read(buf)
		got = append(got, buf[:read]...)
		if err != nil {
			t.Fatalf("after %d of %d bytes: %v", len(got), want, err)
		}
	}
	return got
}

// SIGKILL leaves the broker to say the agent is gone: it publishes the will.
func TestWillAfterSIGKILL(t *testing.T) {
	run := newRun(t)
	agent := run.start(t)
	run.waitFor(t, "a keepalive", func(received message) bool {
		return received.topic == run.agentTopic() && received.str("kind") == "keepalive"
	})

	agent.signal(t, syscall.SIGKILL)
	agent.wait(t, 5*time.Second)
	will := run.waitFor(t, "the will", func(received message) bool {
		return received.topic == run.agentTopic() && received.str("kind") == "offline"
	})
	if will.str("reason") != "will" || will.str("instance_id") != run.instance {
		t.Errorf("offline = %v, want reason will for %s", will.body, run.instance)
	}
}

// With the broker gone mid-run, every reading is recorded in the common log as
// failed, with its data, because nothing else holds it. The v1 defect was a
// record that held only the length.
func TestReadingsRecordedWhenTheBrokerIsGone(t *testing.T) {
	run := newRun(t)
	scans := captureScans(t)
	agent := run.start(t)
	run.waitFor(t, "port_opened", func(received message) bool { return received.isEvent(run.device, "port_opened") })
	run.waitFor(t, "a keepalive", func(received message) bool {
		return received.topic == run.agentTopic() && received.str("kind") == "keepalive"
	})

	run.relay.cut()
	writeAll(t, run.master, scans)
	waitUntil(t, "six failure records", 30*time.Second, func() bool { return len(agent.records(t, "rx publish failed")) == 6 })

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 20*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM with the broker gone, want 0", code)
	}
	for index, record := range agent.records(t, "rx publish failed") {
		want := strings.TrimSuffix(string(scans[index]), "\r\n")
		if record["outcome"] != "failed" || record["data_text"] != want || record["data_hex"] != hex.EncodeToString([]byte(want)) ||
			record["seq"] != float64(index+1) || record["error"] == nil {
			t.Errorf("record %d = %v, want outcome failed, seq %d, the error, and %q as data", index, record, index+1, want)
		}
	}
	if rx := run.matching(func(received message) bool { return received.topic == run.deviceTopic("rx") }); len(rx) != 0 {
		t.Errorf("%d readings reached the broker after it was taken away", len(rx))
	}
}

// records reads the log while the agent may be writing it: a last line
// without its newline is a record still being written, not a broken one, and
// waiting on records must not fail on it.
func TestRecordsLeavesAHalfWrittenRecord(t *testing.T) {
	agent := &agentProcess{logFile: filepath.Join(t.TempDir(), "agent.log")}
	body := `{"msg":"rx published","seq":1}` + "\n" + `{"msg":"rx publ`
	if err := os.WriteFile(agent.logFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := agent.records(t, "rx published"); len(got) != 1 || got[0]["seq"] != float64(1) {
		t.Errorf("records = %v, want the one complete record", got)
	}
}

// run is one test's world: a device, an agent instance and their topics, all
// named after a random run id so that runs, and anything else at the station,
// cannot see each other's messages.
type run struct {
	id       string
	device   string
	instance string
	dir      string
	master   *os.File
	slave    string
	relay    *relay
	// observer is the ingest user's connection, which also sends tx.
	observer *paho.Client

	mu       sync.Mutex
	messages []message
}

func newRun(t *testing.T) *run {
	t.Helper()
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("run id: %v", err)
	}
	id := hex.EncodeToString(random[:])
	agentRun := &run{id: id, device: "e2e-" + id, instance: "e2e-" + id, dir: t.TempDir()}
	agentRun.master, agentRun.slave = newPTY(t)
	agentRun.relay = startRelay(t, os.Getenv("TEST_BROKER"))
	agentRun.subscribe(t)
	return agentRun
}

func (run *run) deviceTopic(leaf string) string {
	return "skuhus/acme/vasby/pack-03/" + run.device + "/" + leaf
}

func (run *run) agentTopic() string {
	return "skuhus/acme/vasby/pack-03/agent/" + run.instance + "/status"
}

// subscribe listens, as the ingest user and straight to the broker, to this
// run's device topics and agent status topic.
func (run *run) subscribe(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", os.Getenv("TEST_BROKER"), 10*time.Second)
	if err != nil {
		t.Fatalf("dial the broker: %v", err)
	}
	client := paho.NewClient(paho.ClientConfig{
		ClientID: "e2e-" + run.id + "-observer",
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(received paho.PublishReceived) (bool, error) {
			run.receive(received.Packet)
			return true, nil
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user, pass := os.Getenv("TEST_INGEST_USER"), os.Getenv("TEST_INGEST_PASS")
	ack, err := client.Connect(ctx, &paho.Connect{ClientID: "e2e-" + run.id + "-observer", KeepAlive: 30, CleanStart: true,
		Username: user, UsernameFlag: true, Password: []byte(pass), PasswordFlag: true})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect as %s: %v, %v", user, err, ack)
	}
	t.Cleanup(func() { _ = client.Disconnect(&paho.Disconnect{}) })
	run.observer = client
	subscription, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "skuhus/acme/vasby/pack-03/" + run.device + "/+", QoS: 1},
		{Topic: run.agentTopic(), QoS: 1},
	}})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for _, code := range subscription.Reasons {
		if code > 1 {
			t.Fatalf("subscribe refused with reason %d", code)
		}
	}
}

// observe has the observer subscribe to filter as well.
func (run *run) observe(t *testing.T, filter string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	subscription, err := run.observer.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}})
	if err != nil {
		t.Fatalf("subscribe to %s: %v", filter, err)
	}
	if len(subscription.Reasons) != 1 || subscription.Reasons[0] > 1 {
		t.Fatalf("subscribe to %s refused: %v", filter, subscription.Reasons)
	}
}

// message is one publish the observer received.
type message struct {
	topic  string
	qos    byte
	expiry *uint32
	body   map[string]any
}

func (received message) str(key string) string { text, _ := received.body[key].(string); return text }
func (received message) num(key string) float64 {
	number, _ := received.body[key].(float64)
	return number
}
func (received message) expiryText() string {
	if received.expiry == nil {
		return "none"
	}
	return fmt.Sprintf("%d s", *received.expiry)
}

func (received message) isEvent(device, code string) bool {
	return received.topic == "skuhus/acme/vasby/pack-03/"+device+"/status" && received.str("kind") == "event" && received.str("code") == code
}

func (received message) firstDevice() (map[string]any, bool) {
	devices, _ := received.body["devices"].([]any)
	if len(devices) == 0 {
		return nil, false
	}
	device, ok := devices[0].(map[string]any)
	return device, ok
}

func (run *run) receive(packet *paho.Publish) {
	received := message{topic: packet.Topic, qos: packet.QoS}
	if packet.Properties != nil {
		received.expiry = packet.Properties.MessageExpiry
	}
	if err := json.Unmarshal(packet.Payload, &received.body); err != nil {
		received.body = map[string]any{"unparsable": string(packet.Payload)}
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	run.messages = append(run.messages, received)
}

func (run *run) matching(match func(message) bool) []message {
	run.mu.Lock()
	defer run.mu.Unlock()
	var out []message
	for _, received := range run.messages {
		if match(received) {
			out = append(out, received)
		}
	}
	return out
}

// waitFor waits for a matching message and returns the first. It does not
// count them: a keepalive, for one, arrives every interval.
func (run *run) waitFor(t *testing.T, what string, match func(message) bool) message {
	t.Helper()
	waitUntil(t, what, 30*time.Second, func() bool { return len(run.matching(match)) > 0 })
	return run.matching(match)[0]
}

// waitForCount waits for count matching messages, and fails the test if more
// than count arrive within a moment: a reading published twice is a defect.
func (run *run) waitForCount(t *testing.T, what string, count int, match func(message) bool) []message {
	t.Helper()
	waitUntil(t, what, 30*time.Second, func() bool { return len(run.matching(match)) >= count })
	time.Sleep(500 * time.Millisecond)
	if got := len(run.matching(match)); got != count {
		t.Fatalf("%s: %d arrived, want exactly %d", what, got, count)
	}
	return run.matching(match)
}

// start writes the agent's configuration, with the run's device, and runs it.
func (run *run) start(t *testing.T) *agentProcess {
	t.Helper()
	return run.startWith(t, deviceConfig(run.device, run.slave, ""))
}

// deviceConfig is one entry of the configuration's devices; extra is more of
// the entry's keys, each line indented as the entry's own.
func deviceConfig(id, path, extra string) string {
	return fmt.Sprintf(`  - id: %s
    path: %s
    separator: "\r\n"
    message_expiry: 30s
    device_type: symbol-05e0-1701
%s`, id, path, extra)
}

// startWith writes the agent's configuration with devices as its devices
// section, and runs it.
func (run *run) startWith(t *testing.T, devices string) *agentProcess {
	t.Helper()
	config := fmt.Sprintf(`identity: { project: acme, site: vasby, station: pack-03, instance: %s }
broker:
  url: tcp://%s
  insecure: true
  reconnect_interval: 100ms
devices:
%sstatus:
  keepalive_interval: 1s
  missed_keepalives: 3
logging:
  level: debug
  file: %s
  stdout: true
`, run.instance, run.relay.addr(), devices, filepath.Join(run.dir, "agent.log"))
	configPath := filepath.Join(run.dir, "agent.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	agent := &agentProcess{logFile: filepath.Join(run.dir, "agent.log"), done: make(chan struct{})}
	agent.cmd = exec.Command(agentBinary, "run", "--config", configPath)
	agent.cmd.Env = append(os.Environ(),
		"SH_DEV_AGENT_MQTT_USERNAME="+os.Getenv("TEST_MQTT_USER"),
		"SH_DEV_AGENT_MQTT_PASSWORD="+os.Getenv("TEST_MQTT_PASS"))
	agent.cmd.Stdout, agent.cmd.Stderr = &agent.stdout, &agent.stderr
	if err := agent.cmd.Start(); err != nil {
		t.Fatalf("start the agent: %v", err)
	}
	go func() {
		agent.err = agent.cmd.Wait()
		close(agent.done)
	}()
	t.Cleanup(func() {
		select {
		case <-agent.done:
		default:
			_ = agent.cmd.Process.Kill()
			<-agent.done
		}
		if strings.Contains(agent.stderr.String(), "DATA RACE") {
			t.Errorf("the race detector found a race in the agent:\n%s", agent.stderr.String())
		}
		if t.Failed() {
			t.Logf("agent stderr:\n%s", agent.stderr.String())
			body, _ := os.ReadFile(agent.logFile)
			t.Logf("agent log:\n%s", body)
		}
	})
	return agent
}

// agentProcess is a running agent and what it wrote.
type agentProcess struct {
	cmd            *exec.Cmd
	stdout, stderr syncBuffer
	logFile        string
	done           chan struct{}
	err            error
}

func (agent *agentProcess) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := agent.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signal %v: %v", sig, err)
	}
}

// wait returns the agent's exit code, or -1 when a signal ended it.
func (agent *agentProcess) wait(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-agent.done:
	case <-time.After(timeout):
		t.Fatalf("the agent did not exit within %s", timeout)
	}
	return agent.cmd.ProcessState.ExitCode()
}

// records returns the log file's records with message msg. It reads the file
// while the agent may still be writing it, so a last line without its newline
// is a record half written, and is left for the next read.
func (agent *agentProcess) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	body, err := os.ReadFile(agent.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read the log: %v", err)
	}
	lines := bytes.Split(body, []byte("\n"))
	// The element after the last newline is empty, or a record still being
	// written.
	lines = lines[:len(lines)-1]
	var out []map[string]any
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		if record["msg"] == msg {
			out = append(out, record)
		}
	}
	return out
}

// relay passes TCP connections from the agent to the broker until cut, which
// closes every connection and stops accepting: to the agent, the broker has
// gone, and the broker itself is untouched.
type relay struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	conns    []net.Conn
	// sessions are the MQTT packets of each connection, one session per
	// connection in the order they were accepted, as they went over the wire.
	sessions []*session
	// isCut is set by cut. A connection accepted just before the cut, still
	// dialling the broker while cut closed the others, checks it and closes
	// itself, so that no connection outlives the cut.
	isCut bool
}

func startRelay(t *testing.T, target string) *relay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	proxy := &relay{listener: listener, target: target}
	go proxy.accept()
	t.Cleanup(proxy.cut)
	return proxy
}

func (relay *relay) addr() string { return relay.listener.Addr().String() }

func (relay *relay) accept() {
	for {
		client, err := relay.listener.Accept()
		if err != nil {
			return
		}
		broker, err := net.DialTimeout("tcp", relay.target, 10*time.Second)
		if err != nil {
			client.Close()
			continue
		}
		relay.mu.Lock()
		if relay.isCut {
			relay.mu.Unlock()
			client.Close()
			broker.Close()
			return
		}
		captured := &session{}
		relay.conns = append(relay.conns, client, broker)
		relay.sessions = append(relay.sessions, captured)
		relay.mu.Unlock()
		// Either side closing closes both, so that the broker sees an agent
		// killed by SIGKILL drop without a DISCONNECT, as it would directly.
		go func() {
			_, _ = io.Copy(broker, io.TeeReader(client, &captured.toBroker))
			client.Close()
			broker.Close()
		}()
		go func() {
			_, _ = io.Copy(client, io.TeeReader(broker, &captured.toAgent))
			client.Close()
			broker.Close()
		}()
	}
}

func (relay *relay) cut() {
	relay.listener.Close()
	relay.mu.Lock()
	defer relay.mu.Unlock()
	relay.isCut = true
	for _, conn := range relay.conns {
		conn.Close()
	}
}

// newPTY opens a pseudo-terminal pair. The ioctls go through SyscallConn, not
// Fd: Fd puts the master in blocking mode, and a blocked read would then keep
// Close from hanging up the slave.
func newPTY(t *testing.T) (*os.File, string) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open /dev/ptmx: %v", err)
	}
	t.Cleanup(func() { master.Close() })
	raw, err := master.SyscallConn()
	if err != nil {
		t.Fatalf("pty: %v", err)
	}
	var number int
	var ioctlErr error
	if err := raw.Control(func(fd uintptr) {
		if ioctlErr = unix.IoctlSetPointerInt(int(fd), unix.TIOCSPTLCK, 0); ioctlErr == nil {
			number, ioctlErr = unix.IoctlGetInt(int(fd), unix.TIOCGPTN)
		}
	}); err != nil || ioctlErr != nil {
		t.Fatalf("pty ioctls: %v, %v", err, ioctlErr)
	}
	return master, fmt.Sprintf("/dev/pts/%d", number)
}

// captureScans splits the capture into its scans, each with its CRLF.
func captureScans(t *testing.T) [][]byte {
	t.Helper()
	body, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatalf("read the capture: %v", err)
	}
	var scans [][]byte
	for _, scan := range bytes.SplitAfter(body, []byte("\r\n")) {
		if len(scan) > 0 {
			scans = append(scans, scan)
		}
	}
	if len(scans) != 6 {
		t.Fatalf("the capture holds %d scans, want 6", len(scans))
	}
	return scans
}

func writeAll(t *testing.T, master *os.File, scans [][]byte) {
	t.Helper()
	if _, err := master.Write(bytes.Join(scans, nil)); err != nil {
		t.Fatalf("write the capture: %v", err)
	}
}

func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// syncBuffer is a bytes.Buffer the agent's output goroutines and the test can
// share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (buffer *syncBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buf.Write(data)
}

func (buffer *syncBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buf.String()
}

// session is one relayed connection's MQTT packets, each way.
type session struct {
	toBroker, toAgent packetLog
}

// packetLog splits one direction of a relayed connection into MQTT packets: a
// type byte, the remaining length as a variable byte integer, and that many
// bytes.
type packetLog struct {
	mu      sync.Mutex
	pending []byte
	packets [][]byte
}

func (stream *packetLog) Write(data []byte) (int, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.pending = append(stream.pending, data...)
	for len(stream.pending) >= 2 {
		remaining, size, complete := readVarint(stream.pending[1:])
		if !complete || len(stream.pending) < 1+size+remaining {
			break
		}
		total := 1 + size + remaining
		stream.packets = append(stream.packets, bytes.Clone(stream.pending[:total]))
		stream.pending = stream.pending[total:]
	}
	return len(data), nil
}

func (stream *packetLog) all() [][]byte {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return append([][]byte(nil), stream.packets...)
}

// readVarint reads an MQTT variable byte integer, and says whether data held
// all of it.
func readVarint(data []byte) (value, size int, complete bool) {
	multiplier := 1
	for index := 0; index < 4 && index < len(data); index++ {
		value += int(data[index]&0x7f) * multiplier
		if data[index]&0x80 == 0 {
			return value, index + 1, true
		}
		multiplier *= 128
	}
	return 0, 0, false
}

// MQTT packet types (MQTT 5.0, section 2.1.2).
const (
	packetSubscribe = 8
	packetSuback    = 9
)

// packetBody is a packet's type and what follows its fixed header.
func packetBody(packet []byte) (byte, []byte) {
	_, size, _ := readVarint(packet[1:])
	return packet[0] >> 4, packet[1+size:]
}

// afterProperties is what follows an MQTT 5 variable header's packet id and
// properties.
func afterProperties(body []byte) []byte {
	length, size, _ := readVarint(body[2:])
	return body[2+size+length:]
}

// subscriptions is what the agent's latest connection subscribed to, as it
// went over the wire: each topic filter in its SUBSCRIBE packets, with the
// reason code the broker's SUBACK of the same packet id gave it.
func (relay *relay) subscriptions(t *testing.T) map[string]int {
	t.Helper()
	relay.mu.Lock()
	if len(relay.sessions) == 0 {
		relay.mu.Unlock()
		t.Fatal("no connection went through the relay")
	}
	latest := relay.sessions[len(relay.sessions)-1]
	relay.mu.Unlock()
	filters := map[uint16][]string{}
	for _, packet := range latest.toBroker.all() {
		if kind, body := packetBody(packet); kind == packetSubscribe {
			var asked []string
			for rest := afterProperties(body); len(rest) >= 2; {
				length := int(binary.BigEndian.Uint16(rest))
				asked = append(asked, string(rest[2:2+length]))
				rest = rest[2+length+1:]
			}
			filters[binary.BigEndian.Uint16(body)] = asked
		}
	}
	answered := map[string]int{}
	for _, packet := range latest.toAgent.all() {
		if kind, body := packetBody(packet); kind == packetSuback {
			codes := afterProperties(body)
			for index, filter := range filters[binary.BigEndian.Uint16(body)] {
				if index < len(codes) {
					answered[filter] = int(codes[index])
				}
			}
		}
	}
	return answered
}

// A tx on a broadcast group's topic reaches every device in the group, at each
// scope, and no other: each writes it to its port and answers on its own
// status topic. The keepalive lists every topic that reaches each device with
// the broker's answer, and those are the topic filters and reason codes that
// went over the wire (#35 Q3a).
//
// It needs the group permissions in dev/rabbitmq/definitions.json: a broker
// started before them refuses the station's project and site group topics.
// make broker-down broker-up loads them.
func TestBroadcastGroupTxReachesEveryDeviceInTheGroup(t *testing.T) {
	run := newRun(t)
	second := run.device + "-b"
	secondMaster, secondSlave := newPTY(t)
	run.observe(t, "skuhus/acme/vasby/pack-03/"+second+"/+")
	group := "e2e-" + run.id
	topics := map[string]string{
		"project": "skuhus/acme/group/" + group + "/tx",
		"site":    "skuhus/acme/vasby/group/" + group + "/tx",
		"station": "skuhus/acme/vasby/pack-03/group/" + group + "/tx",
	}
	agent := run.startWith(t,
		deviceConfig(run.device, run.slave, fmt.Sprintf("    broadcast_groups: { project: [%s], site: [%s] }\n", group, group))+
			deviceConfig(second, secondSlave, fmt.Sprintf("    broadcast_groups: { site: [%s], station: [%s] }\n", group, group)))
	for _, device := range []string{run.device, second} {
		run.waitFor(t, device+"'s port_opened", func(received message) bool { return received.isEvent(device, "port_opened") })
	}

	keepalive := run.waitFor(t, "a keepalive with every tx topic answered", func(received message) bool {
		devices, _ := received.body["devices"].([]any)
		if received.str("kind") != "keepalive" || len(devices) != 2 {
			return false
		}
		for _, entry := range devices {
			txTopics, _ := entry.(map[string]any)["tx_topics"].([]any)
			for _, txTopic := range txTopics {
				if txTopic.(map[string]any)["suback"] == nil {
					return false
				}
			}
		}
		return true
	})
	want := map[string][]string{
		run.device: {"skuhus/acme/vasby/pack-03/" + run.device + "/tx device", topics["project"] + " project " + group, topics["site"] + " site " + group},
		second:     {"skuhus/acme/vasby/pack-03/" + second + "/tx device", topics["site"] + " site " + group, topics["station"] + " station " + group},
	}
	reported := map[string]int{}
	for _, entry := range keepalive.body["devices"].([]any) {
		device := entry.(map[string]any)
		var listed []string
		for _, value := range device["tx_topics"].([]any) {
			txTopic := value.(map[string]any)
			line := fmt.Sprintf("%s %s", txTopic["topic"], txTopic["scope"])
			if txTopic["group"] != nil {
				line += fmt.Sprintf(" %s", txTopic["group"])
			}
			listed = append(listed, line)
			reported[txTopic["topic"].(string)] = int(txTopic["suback"].(float64))
		}
		if id := device["device_id"].(string); strings.Join(listed, "\n") != strings.Join(want[id], "\n") {
			t.Errorf("%s tx_topics:\n  %s\nwant\n  %s", id, strings.Join(listed, "\n  "), strings.Join(want[id], "\n  "))
		}
	}
	onTheWire := run.relay.subscriptions(t)
	if fmt.Sprint(reported) != fmt.Sprint(onTheWire) || len(onTheWire) != 5 {
		t.Errorf("the keepalive reports %v; the agent subscribed, and the broker answered, %v", reported, onTheWire)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	expiry := uint32(30)
	sent := map[string][]byte{}
	ids := map[string]string{
		"site":    "1c9e7a52-3b4d-4f60-8a71-92b3c4d5e6f7",
		"project": "2d0f8b63-4c5e-4a71-9b82-a3c4d5e6f708",
		"station": "3e1a9c74-5d6f-4b82-8c93-b4d5e6f70819",
	}
	for _, scope := range []string{"site", "project", "station"} {
		sent[scope] = []byte("\x05" + scope + " " + run.id + "\n")
		payload, _ := json.Marshal(map[string]any{"schema": 2, "id": ids[scope], "sender": "e2e", "raw_b64": base64.StdEncoding.EncodeToString(sent[scope])})
		if _, err := run.observer.Publish(ctx, &paho.Publish{Topic: topics[scope], QoS: 1, Payload: payload,
			Properties: &paho.PublishProperties{MessageExpiry: &expiry}}); err != nil {
			t.Fatalf("publish the %s group's tx: %v", scope, err)
		}
	}

	for device, scopes := range map[string][]string{run.device: {"site", "project"}, second: {"site", "station"}} {
		master := run.master
		if device == second {
			master = secondMaster
		}
		var expected []byte
		for _, scope := range scopes {
			expected = append(expected, sent[scope]...)
		}
		if got := readExactly(t, master, len(expected), 10*time.Second); !bytes.Equal(got, expected) {
			t.Errorf("%s's port got %q, want %q", device, got, expected)
		}
		if more := readFor(t, master, time.Second); len(more) > 0 {
			t.Errorf("%s's port got %q more, from a group it is not in", device, more)
		}
		for _, scope := range scopes {
			results := run.waitForCount(t, device+"'s results for the "+scope+" group's tx", 2, func(received message) bool {
				return received.topic == "skuhus/acme/vasby/pack-03/"+device+"/status" && received.str("tx_id") == ids[scope]
			})
			if results[0].str("state") != "accepted" || results[1].str("state") != "written" {
				t.Errorf("%s's results for the %s group's tx = %s, %s; want accepted, then written", device, scope,
					results[0].str("state"), results[1].str("state"))
			}
		}
	}

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 15*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM, want 0", code)
	}
	if fanned := agent.records(t, "tx on a broadcast group's topic; each device in the group takes it"); len(fanned) != 3 {
		t.Errorf("%d fan-out records in the log, want one for each group's tx", len(fanned))
	}
}
