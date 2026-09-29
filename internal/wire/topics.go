// Package wire is the agent's contract with everything on the other side of
// the broker: the topics it publishes and listens on, and the JSON of every
// message. DESIGN-V2.md, "Message formats", is the same contract in prose, and
// the tests fail when the two disagree.
package wire

import (
	"fmt"
	"regexp"
)

// segmentRule is what every topic level taken from configuration must match.
// Levels are put into topic names unescaped, so a "/", "+" or "#" in one would
// publish or subscribe outside the station's own topics.
var segmentRule = regexp.MustCompile(`^[a-z0-9-]+$`)

// AgentLevel is the topic level the agent's own status lives under. No device
// may take it as its id, or that device's topics would be the agent's.
const AgentLevel = "agent"

// StationTopics is the topic prefix of one station,
// skuhus/<project>/<site>/<station>, and the topics and filters under it.
type StationTopics struct {
	base string
}

// AgentTopics are the topics of one agent instance at a station.
type AgentTopics struct {
	status string
}

// DeviceTopics are the topics of one device at a station.
type DeviceTopics struct {
	rx, tx, status string
}

// NewStationTopics builds the prefix. It returns an error naming the first
// level that does not follow the segment rule.
func NewStationTopics(project, site, station string) (StationTopics, error) {
	for _, level := range []struct{ name, value string }{
		{"project", project},
		{"site", site},
		{"station", station},
	} {
		if err := checkSegment(level.name, level.value); err != nil {
			return StationTopics{}, err
		}
	}
	return StationTopics{base: "skuhus/" + project + "/" + site + "/" + station}, nil
}

// Agent returns the topics of the agent instance. The instance is a topic
// level, so it follows the segment rule like every other level.
func (station StationTopics) Agent(instance string) (AgentTopics, error) {
	if err := checkSegment("instance", instance); err != nil {
		return AgentTopics{}, err
	}
	return AgentTopics{status: station.base + "/" + AgentLevel + "/" + instance + "/status"}, nil
}

// Device returns the topics of one device. It rejects an id that breaks the
// segment rule, and the reserved id "agent".
func (station StationTopics) Device(id string) (DeviceTopics, error) {
	if err := checkSegment("device id", id); err != nil {
		return DeviceTopics{}, err
	}
	if id == AgentLevel {
		return DeviceTopics{}, fmt.Errorf("device id %q is reserved for the agent's own topics", id)
	}
	prefix := station.base + "/" + id
	return DeviceTopics{rx: prefix + "/rx", tx: prefix + "/tx", status: prefix + "/status"}, nil
}

// EveryDeviceRx is the filter a consumer subscribes to for every device's
// readings at the station.
func (station StationTopics) EveryDeviceRx() string { return station.base + "/+/rx" }

// EveryDeviceStatus is the filter for every device's events and tx results. It
// does not match the agents' status topics, which are one level deeper.
func (station StationTopics) EveryDeviceStatus() string { return station.base + "/+/status" }

// EveryAgentStatus is the filter for every agent instance's keepalives and
// offline messages at the station.
func (station StationTopics) EveryAgentStatus() string {
	return station.base + "/" + AgentLevel + "/+/status"
}

// Status is where the agent publishes its keepalives and offline message, and
// where its will is registered.
func (agent AgentTopics) Status() string { return agent.status }

// Rx carries what the agent read from the device.
func (device DeviceTopics) Rx() string { return device.rx }

// Tx carries what senders want written to the device.
func (device DeviceTopics) Tx() string { return device.tx }

// Status carries the device's events and tx results.
func (device DeviceTopics) Status() string { return device.status }

func checkSegment(name, value string) error {
	if !segmentRule.MatchString(value) {
		return fmt.Errorf("%s %q must match [a-z0-9-]+: it is used verbatim as an MQTT topic level", name, value)
	}
	return nil
}
