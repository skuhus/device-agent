// Package wire is the agent's contract with everything on the other side of
// the broker: the topics it publishes and listens on, and the JSON of every
// message. protocol/asyncapi.yaml and protocol/messages.schema.json publish the
// same contract, and the tests fail when they and this package disagree.
package wire

import (
	"fmt"
	"regexp"
	"strings"
)

// segmentRule is what every topic level taken from configuration must match.
// Levels are put into topic names unescaped, so a "/", "+" or "#" in one would
// publish or subscribe outside the station's own topics.
var segmentRule = regexp.MustCompile(`^[a-z0-9-]+$`)

// AgentLevel is the topic level the agent's own status lives under. No device
// may take it as its id, or that device's topics would be the agent's.
const AgentLevel = "agent"

// The other fixed levels of the agent's topics (DESIGN-V2.md, "Topics").
const (
	// rootLevel opens every topic.
	rootLevel   = "skuhus"
	rxLevel     = "rx"
	txLevel     = "tx"
	statusLevel = "status"
	// anyLevel is MQTT's single-level wildcard, for consumer filters.
	anyLevel = "+"
)

// joinLevels makes a topic, or a filter, of its levels.
func joinLevels(levels ...string) string { return strings.Join(levels, "/") }

// GroupLevel is the topic level broadcast groups' tx topics live under. A
// site's group topic, skuhus/<project>/<site>/group/<group>/tx, has the shape
// of a device's tx topic, so no station may take it as its id (DESIGN-V2.md,
// "Broadcast groups").
const GroupLevel = "group"

// TxScope is how far a tx topic reaches: one device, or every device in a
// broadcast group at a station, a site or a project.
type TxScope string

// The scopes of a tx topic.
const (
	ScopeDevice  TxScope = "device"
	ScopeStation TxScope = "station"
	ScopeSite    TxScope = "site"
	ScopeProject TxScope = "project"
)

// TxRoute is one topic that reaches a device's tx: the device's own, or a
// broadcast group's. Group is empty for the device's own.
type TxRoute struct {
	Topic string
	Scope TxScope
	Group string
}

// StationTopics is the topic prefix of one station,
// skuhus/<project>/<site>/<station>, and the topics and filters under it,
// with the project's and the site's prefixes for broadcast groups.
type StationTopics struct {
	projectPrefix, sitePrefix, stationPrefix string
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
// level that does not follow the segment rule, and rejects the reserved
// station id "group".
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
	if station == GroupLevel {
		return StationTopics{}, fmt.Errorf("station %q is reserved for the site's broadcast group topics", station)
	}
	projectPrefix := joinLevels(rootLevel, project)
	sitePrefix := joinLevels(projectPrefix, site)
	return StationTopics{projectPrefix: projectPrefix, sitePrefix: sitePrefix, stationPrefix: joinLevels(sitePrefix, station)}, nil
}

// GroupTx returns the route of a broadcast group's tx topic: under the
// project, the site or the station, as scope says. The group's name is a topic
// level, so it follows the segment rule.
func (station StationTopics) GroupTx(scope TxScope, group string) (TxRoute, error) {
	if err := checkSegment("broadcast group", group); err != nil {
		return TxRoute{}, err
	}
	var prefix string
	switch scope {
	case ScopeProject:
		prefix = station.projectPrefix
	case ScopeSite:
		prefix = station.sitePrefix
	case ScopeStation:
		prefix = station.stationPrefix
	default:
		return TxRoute{}, fmt.Errorf("broadcast group %q has scope %q; expected project, site or station", group, scope)
	}
	return TxRoute{Topic: joinLevels(prefix, GroupLevel, group, txLevel), Scope: scope, Group: group}, nil
}

// Agent returns the topics of the agent instance. The instance is a topic
// level, so it follows the segment rule like every other level.
func (station StationTopics) Agent(instance string) (AgentTopics, error) {
	if err := checkSegment("instance", instance); err != nil {
		return AgentTopics{}, err
	}
	return AgentTopics{status: joinLevels(station.stationPrefix, AgentLevel, instance, statusLevel)}, nil
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
	prefix := joinLevels(station.stationPrefix, id)
	return DeviceTopics{rx: joinLevels(prefix, rxLevel), tx: joinLevels(prefix, txLevel), status: joinLevels(prefix, statusLevel)}, nil
}

// EveryDeviceRx is the filter a consumer subscribes to for every device's
// readings at the station.
func (station StationTopics) EveryDeviceRx() string {
	return joinLevels(station.stationPrefix, anyLevel, rxLevel)
}

// EveryDeviceStatus is the filter for every device's events and tx results. It
// does not match the agents' status topics, which are one level deeper.
func (station StationTopics) EveryDeviceStatus() string {
	return joinLevels(station.stationPrefix, anyLevel, statusLevel)
}

// EveryAgentStatus is the filter for every agent instance's keepalives and
// offline messages at the station.
func (station StationTopics) EveryAgentStatus() string {
	return joinLevels(station.stationPrefix, AgentLevel, anyLevel, statusLevel)
}

// Status is where the agent publishes its keepalives and offline message, and
// where its will is registered.
func (agent AgentTopics) Status() string { return agent.status }

// Rx carries what the agent read from the device.
func (device DeviceTopics) Rx() string { return device.rx }

// Tx carries what senders want written to the device.
func (device DeviceTopics) Tx() string { return device.tx }

// TxRoute is the device's own tx topic as a route.
func (device DeviceTopics) TxRoute() TxRoute { return TxRoute{Topic: device.tx, Scope: ScopeDevice} }

// Status carries the device's events and tx results.
func (device DeviceTopics) Status() string { return device.status }

func checkSegment(name, value string) error {
	if !segmentRule.MatchString(value) {
		return fmt.Errorf("%s %q must match [a-z0-9-]+: it is used verbatim as an MQTT topic level", name, value)
	}
	return nil
}
