// Command mqtt5spike is the M0 verification described in device-agent-spec.md
// section 5.1: it proves, against a real broker, that the MQTT 5 features the
// agent depends on behave as the specification assumes.
//
// It is a spike rather than part of the agent. Nothing in cmd/skuhus-device-agent
// imports it, and it is not built by "make build". It exists so the answer to
// "does the broker do message expiry" is a measurement rather than a belief,
// and so the measurement can be repeated when the broker is upgraded.
//
// Six checks, plus one that needs a second cluster node, one per property
// that M2 would otherwise assume:
//
//	connack          server capabilities as the broker itself reports them
//	retained         retained publish, replacement, and clearing
//	lwt              last will delivered after an ungraceful disconnect
//	lwt-retained     that will also reaches a subscriber connecting afterwards
//	request-response response topic and correlation data survive a round trip
//	expiry           message expiry interval discards a stale queued message
//	retained-cluster a retained message crosses cluster nodes (only with --peer)
//
// A last check, leftovers, is about the spike rather than the broker: when the
// run ends, nothing a check stored may still be retained under its prefix. A
// clear that fails is reported as cleanup.
//
// Exit status is 0 only when every check passed.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func main() {
	fs := flag.NewFlagSet("mqtt5spike", flag.ContinueOnError)
	broker := fs.String("broker", "", "broker address as host:port, for example 10.9.21.23:1883")
	username := fs.String("username", "", "broker username")
	password := fs.String("password", "", "broker password")
	useTLS := fs.Bool("tls", false, "connect with TLS")
	insecure := fs.Bool("insecure", false, "skip TLS certificate verification (development brokers only)")
	prefix := fs.String("prefix", "skuhus/spike", "topic prefix; a random run id is appended")
	timeout := fs.Duration("timeout", 10*time.Second, "how long to wait for any single expected message")
	willQoS := fs.Uint("will-qos", 1, "QoS of the last will message; brokers have been known to retain wills at one QoS and not another")
	peer := fs.String("peer", "", "a second node of the same cluster as host:port; enables the cross-node retained check")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if *broker == "" {
		fmt.Fprintln(os.Stderr, "error: --broker is required")
		os.Exit(2)
	}

	runner := &runner{
		addr:     *broker,
		username: *username,
		password: *password,
		useTLS:   *useTLS,
		insecure: *insecure,
		runID:    runID(),
		timeout:  *timeout,
		willQoS:  byte(*willQoS),
		peer:     *peer,
		out:      os.Stdout,
	}
	runner.prefix = fmt.Sprintf("%s/%s", strings.TrimSuffix(*prefix, "/"), runner.runID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := runner.run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
	if !runner.report() {
		os.Exit(1)
	}
}

// runID keeps one spike run from colliding with another, and keeps retained
// messages left behind by a killed run out of the next one.
func runID() string {
	var randomBytes [4]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(randomBytes[:])
}

type result struct {
	name   string
	pass   bool
	detail string
}

type runner struct {
	addr     string
	username string
	password string
	useTLS   bool
	insecure bool
	prefix   string
	runID    string
	timeout  time.Duration
	willQoS  byte
	peer     string
	out      io.Writer
	results  []result
}

func (runner *runner) record(name string, pass bool, format string, args ...any) {
	runner.results = append(runner.results, result{name: name, pass: pass, detail: fmt.Sprintf(format, args...)})
	status := "FAIL"
	if pass {
		status = "PASS"
	}
	fmt.Fprintf(runner.out, "%-4s %-16s %s\n", status, name, fmt.Sprintf(format, args...))
}

func (runner *runner) run(ctx context.Context) error {
	fmt.Fprintf(runner.out, "broker %s, topic prefix %s\n\n", runner.addr, runner.prefix)

	// A connection failure is not a failed check: nothing was measured, so the
	// run reports an error rather than a verdict on the broker's properties.
	probe, connack, err := runner.connect(ctx, "probe", connectOptions{cleanStart: true})
	if err != nil {
		return fmt.Errorf("connect to %s: %w", runner.addr, err)
	}
	runner.checkConnack(connack)
	runner.checkRetained(ctx, probe)
	runner.checkRequestResponse(ctx, probe)
	runner.checkRetainedAcrossNodes(ctx, probe)
	probe.close(ctx)

	runner.checkLWT(ctx)
	runner.checkExpiry(ctx)
	runner.checkLeftovers()
	return nil
}

func (runner *runner) report() bool {
	passed, failed := 0, 0
	for _, res := range runner.results {
		if res.pass {
			passed++
			continue
		}
		failed++
	}
	fmt.Fprintf(runner.out, "\n%d passed, %d failed\n", passed, failed)
	return failed == 0
}

// The topics, under the run's prefix, that checks store retained messages on.
// checkLeftovers subscribes to each by name because RabbitMQ 4.3.5 delivers a
// retained message only to a subscription naming its exact topic, not to a
// wildcard subscription (docs/spikes/m0-mqtt5.md).
const (
	statusSuffix        = "/status"
	statusClusterSuffix = "/status-cluster"
	statusLWTSuffix     = "/status-lwt"
)

const (
	// cleanupTimeout bounds each clear and the leftovers check on their own
	// context, independently of the run's.
	cleanupTimeout = 10 * time.Second
	// leftoverWait is how long checkLeftovers listens. It is fixed rather than
	// --timeout, so that a run made to fail with a short timeout is still
	// checked properly.
	leftoverWait = 3 * time.Second
)

// clearRetained removes whatever is stored retained on topic. Every check that
// stores a message defers it, so every return path clears. It connects afresh
// with its own context, because the check's connection may be what failed and
// the run's context running out may be what ended the check. A failed clear
// leaves a message behind, so it is recorded rather than dropped.
func (runner *runner) clearRetained(topic string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	cleaner, _, err := runner.connect(ctx, "cleanup", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("cleanup", false, "connect to clear %s: %v", topic, err)
		return
	}
	defer cleaner.close(ctx)
	if _, err := cleaner.paho.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: true}); err != nil {
		runner.record("cleanup", false, "clear %s: %v", topic, err)
	}
}

// checkLeftovers is the run's evidence that it cleaned up after itself. A
// message still retained on a check's topic is one the check failed to clear,
// or a will the broker stored after the check had already cleared its topic.
// Each is cleared here, so the run leaves nothing behind either way, and the
// check fails if there was any.
func (runner *runner) checkLeftovers() {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	sweeper, _, err := runner.connect(ctx, "leftovers", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("leftovers", false, "connect: %v", err)
		return
	}
	defer sweeper.close(ctx)

	topics := []string{runner.prefix + statusSuffix, runner.prefix + statusClusterSuffix, runner.prefix + statusLWTSuffix}
	for _, topic := range topics {
		if err := sweeper.subscribe(ctx, topic, 1); err != nil {
			runner.record("leftovers", false, "subscribe %s: %v", topic, err)
			return
		}
	}
	var found []string
	for {
		msg, err := sweeper.recv(leftoverWait)
		if err != nil {
			break
		}
		if !msg.Retain {
			continue
		}
		found = append(found, msg.Topic)
		runner.clearRetained(msg.Topic)
	}
	if len(found) > 0 {
		runner.record("leftovers", false, "still retained after the checks, now cleared: %s", strings.Join(found, ", "))
		return
	}
	runner.record("leftovers", true, "nothing retained on %s", strings.Join(topics, ", "))
}

// checkConnack records what the broker says about itself. Nothing here is a
// round trip; it is the server's own declaration, and it is the cheapest way to
// find out that retained messages or QoS 1 are unavailable before the tests
// that depend on them fail confusingly.
func (runner *runner) checkConnack(ca *paho.Connack) {
	if ca.Properties == nil {
		runner.record("connack", false, "no CONNACK properties returned")
		return
	}
	properties := ca.Properties
	// paho fills these from the MQTT defaults when the server omits them, which
	// per the specification means available, so absence and true are the same
	// claim.
	detail := fmt.Sprintf("retain_available=%t max_qos=%s session_expiry=%s wildcard_sub=%t shared_sub=%t sub_id=%t receive_max=%s server_keepalive=%s",
		properties.RetainAvailable, u8(properties.MaximumQoS), u32(properties.SessionExpiryInterval), properties.WildcardSubAvailable,
		properties.SharedSubAvailable, properties.SubIDAvailable, u16(properties.ReceiveMaximum), u16(properties.ServerKeepAlive))

	// The agent publishes scans and status at QoS 1 and relies on retained
	// status, so anything less than that is a fallback decision, not a detail.
	ok := properties.RetainAvailable && (properties.MaximumQoS == nil || *properties.MaximumQoS >= 1)
	runner.record("connack", ok, "%s", detail)
}

// checkRetained covers the status topic in section 5.5: a station that has been
// online since before a consumer started must still be discoverable, which is
// what retained delivery provides, and going offline must be able to clear it.
func (runner *runner) checkRetained(ctx context.Context, pub *client) {
	topic := runner.prefix + statusSuffix

	if _, err := pub.paho.Publish(ctx, &paho.Publish{
		Topic:   topic,
		QoS:     1,
		Retain:  true,
		Payload: []byte(`{"state":"online"}`),
	}); err != nil {
		runner.record("retained", false, "publish retained: %v", err)
		return
	}
	// The clear further down is part of what this check measures. This one is
	// what keeps a check that fails before it from leaving the message behind.
	defer runner.clearRetained(topic)

	// A subscriber that connects after the publish is the case that matters:
	// the broker, not the publisher, has to hold the message.
	sub, _, err := runner.connect(ctx, "retained-sub", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("retained", false, "connect subscriber: %v", err)
		return
	}
	defer sub.close(ctx)

	if err := sub.subscribe(ctx, topic, 1); err != nil {
		runner.record("retained", false, "subscribe: %v", err)
		return
	}
	msg, err := sub.recv(runner.timeout)
	if err != nil {
		runner.record("retained", false, "no retained message on %s: %v", topic, err)
		return
	}
	// The retain flag distinguishes a stored message from a live one. A broker
	// that delivers the payload with the flag clear would make a reconnecting
	// consumer treat an old status as a fresh transition.
	if !msg.Retain {
		runner.record("retained", false, "delivered with retain flag clear, so a stored status is indistinguishable from a live one")
		return
	}

	// Clearing is what a clean shutdown does after publishing offline: a zero
	// length payload removes the stored message rather than storing an empty one.
	if _, err := pub.paho.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: true, Payload: nil}); err != nil {
		runner.record("retained", false, "clear retained: %v", err)
		return
	}
	after, _, err := runner.connect(ctx, "retained-cleared", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("retained", false, "connect second subscriber: %v", err)
		return
	}
	defer after.close(ctx)
	if err := after.subscribe(ctx, topic, 1); err != nil {
		runner.record("retained", false, "subscribe after clear: %v", err)
		return
	}
	if leftover, err := after.recv(2 * time.Second); err == nil {
		runner.record("retained", false, "retained message survived a zero length publish: %q", leftover.Payload)
		return
	}
	runner.record("retained", true, "delivered to a late subscriber with retain=true, and cleared by a zero length publish")
}

// checkRetainedAcrossNodes is the question section 5.1 asks and a single node
// cannot answer: whether a retained message published through one cluster node
// is visible to a consumer connected to another. It runs only when --peer names
// a second node.
//
// If it is not, then which node a consumer happens to reach decides whether it
// learns the station's status, and retained status is not a fleet-wide fact.
func (runner *runner) checkRetainedAcrossNodes(ctx context.Context, pub *client) {
	if runner.peer == "" {
		return
	}
	topic := runner.prefix + statusClusterSuffix

	if _, err := pub.paho.Publish(ctx, &paho.Publish{
		Topic:   topic,
		QoS:     1,
		Retain:  true,
		Payload: []byte(`{"state":"online","node":"primary"}`),
	}); err != nil {
		runner.record("retained-cluster", false, "publish retained on %s: %v", runner.addr, err)
		return
	}
	// Leave no stale retained message behind on the publishing node, however
	// the check ends.
	defer runner.clearRetained(topic)

	sub, _, err := runner.connect(ctx, "retained-peer", connectOptions{cleanStart: true, addr: runner.peer})
	if err != nil {
		runner.record("retained-cluster", false, "connect to peer %s: %v", runner.peer, err)
		return
	}
	defer sub.close(ctx)
	if err := sub.subscribe(ctx, topic, 1); err != nil {
		runner.record("retained-cluster", false, "subscribe on peer: %v", err)
		return
	}
	msg, err := sub.recv(runner.timeout)
	if err != nil {
		// A silent peer proves nothing on its own: it could be the retained
		// store or it could be that ordinary routing does not cross nodes
		// either, which would be a different and larger problem. Publishing a
		// live message to the subscription that is already in place separates
		// the two.
		detail := "retained message published through %s was not delivered to a subscriber on %s"
		if _, perr := pub.paho.Publish(ctx, &paho.Publish{
			Topic:   topic,
			QoS:     1,
			Payload: []byte(`{"live":true}`),
		}); perr == nil {
			if _, lerr := sub.recv(runner.timeout); lerr == nil {
				detail += ", while a live message on the same topic crossed to the same subscriber: the retained store is per node"
			} else {
				detail += ", and neither did a live message, so cross-node routing is broken and this says nothing about the retained store"
			}
		}
		runner.record("retained-cluster", false, detail, runner.addr, runner.peer)
		return
	}
	runner.record("retained-cluster", msg.Retain, "peer %s delivered the retained message with retain=%t", runner.peer, msg.Retain)
}

// checkLWT covers section 5.5: the broker must publish the will when the agent
// dies without disconnecting, which is the case that makes "station 3 went
// offline" free. A graceful disconnect must not produce one.
func (runner *runner) checkLWT(ctx context.Context) {
	topic := runner.prefix + statusLWTSuffix

	observer, _, err := runner.connect(ctx, "lwt-observer", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("lwt", false, "connect observer: %v", err)
		return
	}
	defer observer.close(ctx)
	if err := observer.subscribe(ctx, topic, 1); err != nil {
		runner.record("lwt", false, "subscribe: %v", err)
		return
	}

	var delay uint32 // Deliver the will immediately; a delay would only measure the timer.
	dying, _, err := runner.connect(ctx, "lwt-victim", connectOptions{
		cleanStart: true,
		will: &paho.WillMessage{
			Topic:   topic,
			QoS:     runner.willQoS,
			Retain:  true,
			Payload: []byte(`{"state":"offline","reason":"will"}`),
		},
		willProps: &paho.WillProperties{WillDelayInterval: &delay},
	})
	if err != nil {
		runner.record("lwt", false, "connect client with will: %v", err)
		return
	}
	// From the socket close below, a broker that retains wills stores this one.
	// The clear covers checkWillRetained as well, which runs inside this
	// function, and every return in between.
	defer runner.clearRetained(topic)

	// Closing the socket without sending DISCONNECT is what a killed process or
	// a pulled network cable looks like to the broker. A graceful Disconnect
	// would discard the will, which is exactly what the check must not do.
	if err := dying.conn.Close(); err != nil {
		runner.record("lwt", false, "close connection: %v", err)
		return
	}

	msg, err := observer.recv(runner.timeout)
	if err != nil {
		runner.record("lwt", false, "no will published within %s of an ungraceful disconnect: %v", runner.timeout, err)
		return
	}
	if !bytes.Contains(msg.Payload, []byte("will")) {
		runner.record("lwt", false, "unexpected payload on the will topic: %q", msg.Payload)
		return
	}
	// The delivered retain flag is expected to be clear here whatever the broker
	// stored: this subscription did not ask for retain as published, so the
	// flag says nothing about the retained store. checkWillRetained asks that
	// question properly.
	runner.record("lwt", true, "will delivered to a live subscriber after socket close (will qos %d)", runner.willQoS)

	runner.checkWillRetained(ctx, topic)
}

// checkWillRetained is the half of section 5.5 that a live subscription cannot
// show. Status is retained so that a consumer starting later still learns the
// station state; if the broker delivers a will but does not retain it, the last
// retained status of a dead station stays "online" forever and the offline
// transition is visible only to whoever was connected at the time.
func (runner *runner) checkWillRetained(ctx context.Context, topic string) {
	late, _, err := runner.connect(ctx, "lwt-late", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("lwt-retained", false, "connect late subscriber: %v", err)
		return
	}
	defer late.close(ctx)
	if err := late.subscribe(ctx, topic, 1); err != nil {
		runner.record("lwt-retained", false, "subscribe: %v", err)
		return
	}
	msg, err := late.recv(runner.timeout)
	if err != nil {
		runner.record("lwt-retained", false, "a subscriber connecting after the will was published received nothing (will qos %d), so a dead station's last retained status stays online", runner.willQoS)
		return
	}
	if !msg.Retain {
		runner.record("lwt-retained", false, "message arrived without the retain flag, so it did not come from the retained store")
		return
	}
	runner.record("lwt-retained", true, "the will was stored retained and reached a subscriber that connected afterwards: %q", msg.Payload)
}

// checkRequestResponse covers the command channel in section 5.7: a reply has
// to be matchable to its request, and the requester has to be able to name the
// topic it is listening on. Both are MQTT 5 properties the broker must relay
// untouched.
func (runner *runner) checkRequestResponse(ctx context.Context, requester *client) {
	cmdTopic := runner.prefix + "/cmd"
	replyTopic := runner.prefix + "/cmd/result"
	correlation := []byte("corr-" + runner.runID)

	agent, _, err := runner.connect(ctx, "rr-agent", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("request-response", false, "connect agent: %v", err)
		return
	}
	defer agent.close(ctx)
	if err := agent.subscribe(ctx, cmdTopic, 1); err != nil {
		runner.record("request-response", false, "subscribe cmd: %v", err)
		return
	}
	if err := requester.subscribe(ctx, replyTopic, 1); err != nil {
		runner.record("request-response", false, "subscribe result: %v", err)
		return
	}

	if _, err := requester.paho.Publish(ctx, &paho.Publish{
		Topic:   cmdTopic,
		QoS:     1,
		Payload: []byte(`{"cmd":"ping"}`),
		Properties: &paho.PublishProperties{
			ResponseTopic:   replyTopic,
			CorrelationData: correlation,
		},
	}); err != nil {
		runner.record("request-response", false, "publish command: %v", err)
		return
	}

	req, err := agent.recv(runner.timeout)
	if err != nil {
		runner.record("request-response", false, "command not received: %v", err)
		return
	}
	if req.Properties == nil || req.Properties.ResponseTopic != replyTopic {
		runner.record("request-response", false, "response topic did not survive the broker: got %q", responseTopic(req))
		return
	}
	if !bytes.Equal(req.Properties.CorrelationData, correlation) {
		runner.record("request-response", false, "correlation data did not survive the broker: got %q", req.Properties.CorrelationData)
		return
	}

	// Replying on the topic the request named, rather than a topic agreed in
	// advance, is the half of the pattern a broker can break on its own.
	if _, err := agent.paho.Publish(ctx, &paho.Publish{
		Topic:   req.Properties.ResponseTopic,
		QoS:     1,
		Payload: []byte(`{"result":"pong"}`),
		Properties: &paho.PublishProperties{
			CorrelationData: req.Properties.CorrelationData,
		},
	}); err != nil {
		runner.record("request-response", false, "publish reply: %v", err)
		return
	}
	reply, err := requester.recv(runner.timeout)
	if err != nil {
		runner.record("request-response", false, "reply not received on %s: %v", replyTopic, err)
		return
	}
	if reply.Properties == nil || !bytes.Equal(reply.Properties.CorrelationData, correlation) {
		runner.record("request-response", false, "reply carried no matching correlation data")
		return
	}
	runner.record("request-response", true, "response topic and correlation data relayed unchanged in both directions")
}

// checkExpiry covers section 6, which is the reason this agent has no offline
// buffer: a scan that outlived its session must be discarded by the broker
// rather than delivered late. The check queues two messages for an offline
// session, one short lived and one not, and asserts only the second arrives.
func (runner *runner) checkExpiry(ctx context.Context) {
	topic := runner.prefix + "/scan"
	const (
		shortExpiry = 2   // seconds; must expire while the subscriber is away
		longExpiry  = 300 // seconds; must survive the same absence
		absence     = 6 * time.Second
	)

	sessionExpiry := uint32(300)
	sub, _, err := runner.connect(ctx, "expiry-sub", connectOptions{
		cleanStart:    true, // Start from a known empty session, then keep it below.
		sessionExpiry: &sessionExpiry,
	})
	if err != nil {
		runner.record("expiry", false, "connect subscriber: %v", err)
		return
	}
	if err := sub.subscribe(ctx, topic, 1); err != nil {
		runner.record("expiry", false, "subscribe: %v", err)
		sub.close(ctx)
		return
	}
	// Disconnecting while the session expiry interval is non-zero leaves the
	// subscription in place, so the broker has to hold QoS 1 messages for it.
	// This is not how the agent connects - it uses clean start with session
	// expiry zero, section 5.2 - but it is the only way to make the broker
	// queue a message long enough for expiry to be observable.
	sub.close(ctx)

	pub, _, err := runner.connect(ctx, "expiry-pub", connectOptions{cleanStart: true})
	if err != nil {
		runner.record("expiry", false, "connect publisher: %v", err)
		return
	}
	short, long := uint32(shortExpiry), uint32(longExpiry)
	for _, message := range []struct {
		payload string
		expiry  *uint32
	}{
		{"stale", &short},
		{"fresh", &long},
	} {
		if _, err := pub.paho.Publish(ctx, &paho.Publish{
			Topic:      topic,
			QoS:        1,
			Payload:    []byte(message.payload),
			Properties: &paho.PublishProperties{MessageExpiry: message.expiry},
		}); err != nil {
			runner.record("expiry", false, "publish %s: %v", message.payload, err)
			pub.close(ctx)
			return
		}
	}
	pub.close(ctx)

	select {
	case <-time.After(absence):
	case <-ctx.Done():
		runner.record("expiry", false, "run cancelled while waiting out the expiry")
		return
	}

	back, connack, err := runner.connect(ctx, "expiry-sub", connectOptions{
		cleanStart:    false,
		sessionExpiry: &sessionExpiry,
	})
	if err != nil {
		runner.record("expiry", false, "reconnect subscriber: %v", err)
		return
	}
	defer back.close(ctx)
	if !connack.SessionPresent {
		// Without the session the broker never queued anything, so the absence
		// of the stale message proves nothing about expiry.
		runner.record("expiry", false, "session not present on reconnect, so nothing was queued and expiry was not exercised")
		return
	}

	var delivered []string
	deadline := time.Now().Add(runner.timeout)
	for {
		msg, err := back.recv(time.Until(deadline))
		if err != nil {
			break
		}
		remaining := ""
		if msg.Properties != nil && msg.Properties.MessageExpiry != nil {
			remaining = fmt.Sprintf(" (remaining expiry %ds)", *msg.Properties.MessageExpiry)
		}
		delivered = append(delivered, string(msg.Payload)+remaining)
		if len(delivered) > 1 {
			break
		}
	}

	switch {
	case len(delivered) == 0:
		runner.record("expiry", false, "neither message was delivered, so the broker did not queue for an offline session at all")
	case len(delivered) == 1 && strings.HasPrefix(delivered[0], "fresh"):
		runner.record("expiry", true, "the %ds message expired during a %s absence, the %ds one was delivered: %s",
			shortExpiry, absence, longExpiry, delivered[0])
	default:
		runner.record("expiry", false, "expected only the long lived message, got %v", delivered)
	}
}

func responseTopic(publish *paho.Publish) string {
	if publish.Properties == nil {
		return ""
	}
	return publish.Properties.ResponseTopic
}

func u8(value *byte) string {
	if value == nil {
		return "unset"
	}
	return fmt.Sprintf("%d", *value)
}

func u16(value *uint16) string {
	if value == nil {
		return "unset"
	}
	return fmt.Sprintf("%d", *value)
}

func u32(value *uint32) string {
	if value == nil {
		return "unset"
	}
	return fmt.Sprintf("%d", *value)
}

type connectOptions struct {
	cleanStart    bool
	sessionExpiry *uint32
	will          *paho.WillMessage
	willProps     *paho.WillProperties
	// addr overrides the broker address, which is what the cross-node retained
	// check needs. Empty means the address the run was started with.
	addr string
}

// client is one MQTT connection plus the messages it has received. The net.Conn
// is kept because the will check needs to close the socket without sending a
// DISCONNECT, which the paho API deliberately does not offer.
type client struct {
	paho *paho.Client
	conn net.Conn
	msgs chan *paho.Publish
}

func (runner *runner) connect(ctx context.Context, name string, opts connectOptions) (*client, *paho.Connack, error) {
	addr := opts.addr
	if addr == "" {
		addr = runner.addr
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	var err error
	if runner.useTLS {
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, nil, fmt.Errorf("broker address %q: %w", addr, splitErr)
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: runner.insecure,
		})
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, nil, err
	}

	msgs := make(chan *paho.Publish, 32)
	pahoClient := paho.NewClient(paho.ClientConfig{
		ClientID: fmt.Sprintf("spike-%s-%s", runner.runID, name),
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				select {
				case msgs <- pr.Packet:
				default: // A full channel means the check is not reading; losing the message is better than blocking the client's read loop.
				}
				return true, nil
			},
		},
		OnClientError: func(error) {}, // Every check ends by closing a connection, so these are expected.
	})

	cp := &paho.Connect{
		ClientID:       pahoClient.ClientID(),
		KeepAlive:      30,
		CleanStart:     opts.cleanStart,
		WillMessage:    opts.will,
		WillProperties: opts.willProps,
	}
	if runner.username != "" {
		cp.Username, cp.UsernameFlag = runner.username, true
		cp.Password, cp.PasswordFlag = []byte(runner.password), true
	}
	if opts.sessionExpiry != nil {
		cp.Properties = &paho.ConnectProperties{SessionExpiryInterval: opts.sessionExpiry}
	}

	ca, err := pahoClient.Connect(ctx, cp)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, nil, fmt.Errorf("CONNACK reason %d: %s", ca.ReasonCode, connackReason(ca))
	}
	return &client{paho: pahoClient, conn: conn, msgs: msgs}, ca, nil
}

func connackReason(ca *paho.Connack) string {
	if ca.Properties != nil && ca.Properties.ReasonString != "" {
		return ca.Properties.ReasonString
	}
	return "no reason string returned"
}

func (client *client) subscribe(ctx context.Context, topic string, qos byte) error {
	sa, err := client.paho.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: qos}},
	})
	if err != nil {
		return err
	}
	for _, code := range sa.Reasons {
		if code > 2 {
			return fmt.Errorf("SUBACK refused %s with reason %d", topic, code)
		}
	}
	return nil
}

var errTimeout = errors.New("timed out")

func (client *client) recv(timeout time.Duration) (*paho.Publish, error) {
	if timeout <= 0 {
		return nil, errTimeout
	}
	select {
	case msg := <-client.msgs:
		return msg, nil
	case <-time.After(timeout):
		return nil, errTimeout
	}
}

func (client *client) close(ctx context.Context) {
	_ = ctx
	if err := client.paho.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
		client.conn.Close()
	}
}
