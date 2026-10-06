package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// subscriber is the part of the connection manager that subscribe uses, so
// that a test can answer for the broker.
type subscriber interface {
	Subscribe(ctx context.Context, packet *paho.Subscribe) (*paho.Suback, error)
}

// connectionUp subscribes a connection that has just come up, records the
// broker's answers under the connection's number, and then reports the
// connection up.
func connectionUp(manager subscriber, opts Options, answers *subscribeAnswers, connection uint64, log *slog.Logger) {
	if len(opts.Subscriptions) > 0 {
		for filter, code := range subscribe(manager, opts.Subscriptions, opts.ConnectTimeout, log) {
			if !answers.record(connection, filter, code) {
				log.Debug("subscription answer not recorded: a newer connection has come up", "topic", filter,
					"reason", fmt.Sprintf("0x%02x", code))
			}
		}
	}
	if opts.OnUp != nil {
		opts.OnUp()
	}
}

// subscribe asks for every topic at QoS 1, waiting at most timeout for the
// answer, logs what the broker answered, and returns the answer to each topic
// it answered, keyed by the topic filter as it went into the packet. A refused
// topic is an ERROR: no tx arrives on it, the usual cause, the station's topic
// permission, is the operator's to fix, and RabbitMQ 4.3.5 then closes the
// connection (DESIGN-V2.md, "Broadcast groups").
//
// paho returns the SUBACK together with an error when the broker refused any
// topic (paho/client.go, Subscribe, in paho.golang v0.23.0), so an error with
// a SUBACK is an answer, and only an error without one is a failure to
// subscribe.
func subscribe(manager subscriber, topics []string, timeout time.Duration, log *slog.Logger) map[string]byte {
	subscriptions := make([]paho.SubscribeOptions, 0, len(topics))
	for _, topic := range topics {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: topic, QoS: qosAtLeastOnce})
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	suback, err := manager.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil && suback == nil {
		log.Error("subscribing failed; no tx will arrive until the next connection", "topics", topics,
			"timeout", timeout.String(), "error", err.Error())
		return nil
	}
	answered := make(map[string]byte, len(subscriptions))
	for index, subscription := range subscriptions {
		if index >= len(suback.Reasons) {
			log.Error("the broker answered fewer subscriptions than were asked for", "topic", subscription.Topic, "answers", len(suback.Reasons))
			continue
		}
		code := suback.Reasons[index]
		answered[subscription.Topic] = code
		if code >= reasonCodeFailure {
			log.Error("subscription refused; no tx will arrive on this topic", "topic", subscription.Topic, "reason", fmt.Sprintf("0x%02x", code))
			continue
		}
		log.Info("subscribed", "topic", subscription.Topic, "qos", code)
	}
	return answered
}

// subscribeAnswers holds the broker's answer to each topic filter the current
// connection subscribed to, keyed by the filter as it went into the SUBSCRIBE
// packet. Each connection starts it empty: the session is clean, so nothing
// answered on an earlier connection still holds.
type subscribeAnswers struct {
	mu sync.Mutex
	// connection numbers the connections as they come up.
	connection uint64
	byFilter   map[string]byte
}

// begin empties the answers for a connection that has come up, and returns
// the number its answers are recorded under.
func (answers *subscribeAnswers) begin() uint64 {
	answers.mu.Lock()
	defer answers.mu.Unlock()
	answers.connection++
	answers.byFilter = map[string]byte{}
	return answers.connection
}

// record keeps the broker's answer to filter, unless a newer connection has
// come up since connection's SUBSCRIBE was sent. It reports whether it kept it.
func (answers *subscribeAnswers) record(connection uint64, filter string, code byte) bool {
	answers.mu.Lock()
	defer answers.mu.Unlock()
	if connection != answers.connection {
		return false
	}
	answers.byFilter[filter] = code
	return true
}

// snapshot is a copy of the current connection's answers.
func (answers *subscribeAnswers) snapshot() map[string]byte {
	answers.mu.Lock()
	defer answers.mu.Unlock()
	return maps.Clone(answers.byFilter)
}

// SubscribeAnswers is the broker's answer to each topic filter the current
// connection subscribed to, keyed by the filter as it went into the SUBSCRIBE
// packet: a granted QoS, 0 to 2, or a refusal, 0x80 and above. A filter the
// broker has not answered on this connection is absent.
func (client *Client) SubscribeAnswers() map[string]byte {
	return client.answers.snapshot()
}
