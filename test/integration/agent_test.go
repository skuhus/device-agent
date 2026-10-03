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

func TestMain(m *testing.M) {
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
	code := m.Run()
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

	run.waitFor(t, "port_opened", func(m message) bool { return m.isEvent(run.device, "port_opened") })
	writeAll(t, run.master, scans)

	rx := run.waitForCount(t, "six readings", 6, func(m message) bool { return m.topic == run.deviceTopic("rx") })
	for i, m := range rx {
		raw, err := base64.StdEncoding.DecodeString(m.str("raw_b64"))
		want := strings.TrimSuffix(string(scans[i]), "\r\n")
		if err != nil || string(raw) != want || m.num("seq") != float64(i+1) || m.str("device_type") != "symbol-05e0-1701" {
			t.Errorf("rx %d = %q seq %v type %q, want %q seq %d, symbol-05e0-1701", i, raw, m.num("seq"), m.str("device_type"), want, i+1)
		}
		if m.qos != 1 || m.expiry == nil || *m.expiry < 25 || *m.expiry > 30 {
			t.Errorf("rx %d arrived at QoS %d with expiry %s, want QoS 1 and the device's 30 s, less transit", i, m.qos, m.expiryText())
		}
	}

	captured := 0
	for _, scan := range scans {
		captured += len(scan)
	}
	keepalive := run.waitFor(t, "a keepalive with every reading counted", func(m message) bool {
		device, ok := m.firstDevice()
		return m.str("kind") == "keepalive" && ok && device["rx_frames"] == float64(6)
	})
	device, _ := keepalive.firstDevice()
	if device["device_id"] != run.device || device["rx_bytes"] != float64(captured) || device["device_open"] != true ||
		keepalive.num("interval_s") != 1 || keepalive.num("gone_after_s") != 3 {
		t.Errorf("keepalive = %v, device %v; want %s open, %d bytes read, interval 1 s, gone after 3 s", keepalive.body, device, run.device, captured)
	}

	run.master.Close()
	lost := run.waitFor(t, "port_lost", func(m message) bool { return m.isEvent(run.device, "port_lost") })
	if detail, _ := lost.body["detail"].(map[string]any); detail["error_class"] != "disconnected" || lost.body["device_open"] != false {
		t.Errorf("port_lost = %v, want class disconnected and the port closed", lost.body)
	}

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 15*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM, want 0", code)
	}
	offline := run.waitFor(t, "the offline message", func(m message) bool { return m.topic == run.agentTopic() && m.str("kind") == "offline" })
	if offline.str("reason") != "shutdown" || offline.qos != 1 {
		t.Errorf("offline = %v at QoS %d, want reason shutdown at QoS 1", offline.body, offline.qos)
	}
	time.Sleep(2 * time.Second)
	if wills := run.matching(func(m message) bool { return m.topic == run.agentTopic() && m.str("reason") == "will" }); len(wills) > 0 {
		t.Errorf("the broker published the will after a clean stop: %v", wills[0].body)
	}
	if published := agent.records(t, "rx published"); len(published) != 6 {
		t.Errorf("%d published records in the log, want 6", len(published))
	}
}

// SIGKILL leaves the broker to say the agent is gone: it publishes the will.
func TestWillAfterSIGKILL(t *testing.T) {
	run := newRun(t)
	agent := run.start(t)
	run.waitFor(t, "a keepalive", func(m message) bool { return m.topic == run.agentTopic() && m.str("kind") == "keepalive" })

	agent.signal(t, syscall.SIGKILL)
	agent.wait(t, 5*time.Second)
	will := run.waitFor(t, "the will", func(m message) bool { return m.topic == run.agentTopic() && m.str("kind") == "offline" })
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
	run.waitFor(t, "port_opened", func(m message) bool { return m.isEvent(run.device, "port_opened") })
	run.waitFor(t, "a keepalive", func(m message) bool { return m.topic == run.agentTopic() && m.str("kind") == "keepalive" })

	run.relay.cut()
	writeAll(t, run.master, scans)
	waitUntil(t, "six failure records", 30*time.Second, func() bool { return len(agent.records(t, "rx publish failed")) == 6 })

	agent.signal(t, syscall.SIGTERM)
	if code := agent.wait(t, 20*time.Second); code != 0 {
		t.Errorf("the agent exited %d on SIGTERM with the broker gone, want 0", code)
	}
	for i, record := range agent.records(t, "rx publish failed") {
		want := strings.TrimSuffix(string(scans[i]), "\r\n")
		if record["outcome"] != "failed" || record["data_text"] != want || record["data_hex"] != hex.EncodeToString([]byte(want)) ||
			record["seq"] != float64(i+1) || record["error"] == nil {
			t.Errorf("record %d = %v, want outcome failed, seq %d, the error, and %q as data", i, record, i+1, want)
		}
	}
	if rx := run.matching(func(m message) bool { return m.topic == run.deviceTopic("rx") }); len(rx) != 0 {
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
	r := &run{id: id, device: "e2e-" + id, instance: "e2e-" + id, dir: t.TempDir()}
	r.master, r.slave = newPTY(t)
	r.relay = startRelay(t, os.Getenv("TEST_BROKER"))
	r.subscribe(t)
	return r
}

func (r *run) deviceTopic(leaf string) string {
	return "skuhus/acme/vasby/pack-03/" + r.device + "/" + leaf
}

func (r *run) agentTopic() string { return "skuhus/acme/vasby/pack-03/agent/" + r.instance + "/status" }

// subscribe listens, as the ingest user and straight to the broker, to this
// run's device topics and agent status topic.
func (r *run) subscribe(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", os.Getenv("TEST_BROKER"), 10*time.Second)
	if err != nil {
		t.Fatalf("dial the broker: %v", err)
	}
	client := paho.NewClient(paho.ClientConfig{
		ClientID: "e2e-" + r.id + "-observer",
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(received paho.PublishReceived) (bool, error) {
			r.receive(received.Packet)
			return true, nil
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user, pass := os.Getenv("TEST_INGEST_USER"), os.Getenv("TEST_INGEST_PASS")
	ack, err := client.Connect(ctx, &paho.Connect{ClientID: "e2e-" + r.id + "-observer", KeepAlive: 30, CleanStart: true,
		Username: user, UsernameFlag: true, Password: []byte(pass), PasswordFlag: true})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect as %s: %v, %v", user, err, ack)
	}
	t.Cleanup(func() { _ = client.Disconnect(&paho.Disconnect{}) })
	subscription, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "skuhus/acme/vasby/pack-03/" + r.device + "/+", QoS: 1},
		{Topic: r.agentTopic(), QoS: 1},
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

// message is one publish the observer received.
type message struct {
	topic  string
	qos    byte
	expiry *uint32
	body   map[string]any
}

func (m message) str(key string) string  { s, _ := m.body[key].(string); return s }
func (m message) num(key string) float64 { f, _ := m.body[key].(float64); return f }
func (m message) expiryText() string {
	if m.expiry == nil {
		return "none"
	}
	return fmt.Sprintf("%d s", *m.expiry)
}

func (m message) isEvent(device, code string) bool {
	return m.topic == "skuhus/acme/vasby/pack-03/"+device+"/status" && m.str("kind") == "event" && m.str("code") == code
}

func (m message) firstDevice() (map[string]any, bool) {
	devices, _ := m.body["devices"].([]any)
	if len(devices) == 0 {
		return nil, false
	}
	device, ok := devices[0].(map[string]any)
	return device, ok
}

func (r *run) receive(packet *paho.Publish) {
	m := message{topic: packet.Topic, qos: packet.QoS}
	if packet.Properties != nil {
		m.expiry = packet.Properties.MessageExpiry
	}
	if err := json.Unmarshal(packet.Payload, &m.body); err != nil {
		m.body = map[string]any{"unparsable": string(packet.Payload)}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, m)
}

func (r *run) matching(match func(message) bool) []message {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []message
	for _, m := range r.messages {
		if match(m) {
			out = append(out, m)
		}
	}
	return out
}

func (r *run) waitFor(t *testing.T, what string, match func(message) bool) message {
	t.Helper()
	return r.waitForCount(t, what, 1, match)[0]
}

// waitForCount waits for count matching messages, and fails the test if more
// arrive within a moment, since every check here counts exact deliveries.
func (r *run) waitForCount(t *testing.T, what string, count int, match func(message) bool) []message {
	t.Helper()
	waitUntil(t, what, 30*time.Second, func() bool { return len(r.matching(match)) >= count })
	if count > 1 {
		time.Sleep(500 * time.Millisecond)
		if got := len(r.matching(match)); got != count {
			t.Fatalf("%s: %d arrived, want exactly %d", what, got, count)
		}
	}
	return r.matching(match)
}

// start writes the agent's configuration and runs it.
func (r *run) start(t *testing.T) *agentProcess {
	t.Helper()
	config := fmt.Sprintf(`identity: { project: acme, site: vasby, station: pack-03, instance: %s }
broker:
  url: tcp://%s
  insecure: true
  connect_backoff: { initial: 100ms, max: 500ms }
devices:
  - id: %s
    path: %s
    separator: "\r\n"
    message_expiry: 30s
    device_type: symbol-05e0-1701
status:
  keepalive_interval: 1s
  missed_keepalives: 3
logging:
  level: debug
  file: %s
  stdout: true
`, r.instance, r.relay.addr(), r.device, r.slave, filepath.Join(r.dir, "agent.log"))
	configPath := filepath.Join(r.dir, "agent.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	agent := &agentProcess{logFile: filepath.Join(r.dir, "agent.log"), done: make(chan struct{})}
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
	r := &relay{listener: listener, target: target}
	go r.accept()
	t.Cleanup(r.cut)
	return r
}

func (r *relay) addr() string { return r.listener.Addr().String() }

func (r *relay) accept() {
	for {
		client, err := r.listener.Accept()
		if err != nil {
			return
		}
		broker, err := net.DialTimeout("tcp", r.target, 10*time.Second)
		if err != nil {
			client.Close()
			continue
		}
		r.mu.Lock()
		if r.isCut {
			r.mu.Unlock()
			client.Close()
			broker.Close()
			return
		}
		r.conns = append(r.conns, client, broker)
		r.mu.Unlock()
		// Either side closing closes both, so that the broker sees an agent
		// killed by SIGKILL drop without a DISCONNECT, as it would directly.
		go func() {
			_, _ = io.Copy(broker, client)
			client.Close()
			broker.Close()
		}()
		go func() {
			_, _ = io.Copy(client, broker)
			client.Close()
			broker.Close()
		}()
	}
}

func (r *relay) cut() {
	r.listener.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isCut = true
	for _, conn := range r.conns {
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

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
