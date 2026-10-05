package wire

import (
	"strings"
	"testing"
)

func mustStation(t *testing.T) StationTopics {
	t.Helper()
	station, err := NewStationTopics("acme", "vasby", "pack-03")
	if err != nil {
		t.Fatalf("NewStationTopics: %v", err)
	}
	return station
}

func TestTopicNames(t *testing.T) {
	station := mustStation(t)
	agent, err := station.Agent("pack-03")
	if err != nil {
		t.Fatalf("Agent: %v", err)
	}
	device, err := station.Device("scanner-1")
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	for _, check := range []struct{ name, got, want string }{
		{"agent status", agent.Status(), "skuhus/acme/vasby/pack-03/agent/pack-03/status"},
		{"device rx", device.Rx(), "skuhus/acme/vasby/pack-03/scanner-1/rx"},
		{"device tx", device.Tx(), "skuhus/acme/vasby/pack-03/scanner-1/tx"},
		{"device status", device.Status(), "skuhus/acme/vasby/pack-03/scanner-1/status"},
		{"every device rx", station.EveryDeviceRx(), "skuhus/acme/vasby/pack-03/+/rx"},
		{"every device status", station.EveryDeviceStatus(), "skuhus/acme/vasby/pack-03/+/status"},
		{"every agent status", station.EveryAgentStatus(), "skuhus/acme/vasby/pack-03/agent/+/status"},
	} {
		if check.got != check.want {
			t.Errorf("%s = %q, want %q", check.name, check.got, check.want)
		}
	}
}

// A level outside the segment rule would put a message somewhere other than
// its own topic: "/" adds a level, "+" and "#" are wildcards, and "." is how
// RabbitMQ separates levels in the routing keys its permissions match.
func TestSegmentRuleRejectsEveryLevel(t *testing.T) {
	bad := []string{"", "Pack-03", "pack_03", "pack.03", "pack/03", "pack+03", "pack#03", "pack 03"}
	for _, value := range bad {
		if _, err := NewStationTopics(value, "vasby", "pack-03"); err == nil || !strings.Contains(err.Error(), "project") {
			t.Errorf("project %q: err = %v, want a rejection naming project", value, err)
		}
		if _, err := NewStationTopics("acme", value, "pack-03"); err == nil || !strings.Contains(err.Error(), "site") {
			t.Errorf("site %q: err = %v, want a rejection naming site", value, err)
		}
		if _, err := NewStationTopics("acme", "vasby", value); err == nil || !strings.Contains(err.Error(), "station") {
			t.Errorf("station %q: err = %v, want a rejection naming station", value, err)
		}
		station := mustStation(t)
		if _, err := station.Agent(value); err == nil || !strings.Contains(err.Error(), "instance") {
			t.Errorf("instance %q: err = %v, want a rejection naming instance", value, err)
		}
		if _, err := station.Device(value); err == nil || !strings.Contains(err.Error(), "device id") {
			t.Errorf("device id %q: err = %v, want a rejection naming device id", value, err)
		}
		if _, err := station.GroupTx(ScopeSite, value); err == nil || !strings.Contains(err.Error(), "broadcast group") {
			t.Errorf("broadcast group %q: err = %v, want a rejection naming broadcast group", value, err)
		}
	}
}

// A group's tx topic is under the project, the site or the station, so the
// same name at two scopes is two topics. The device's own topic is a route too,
// with no group.
func TestBroadcastGroupTopics(t *testing.T) {
	station := mustStation(t)
	for _, check := range []struct {
		scope TxScope
		want  string
	}{
		{ScopeProject, "skuhus/acme/group/scales/tx"},
		{ScopeSite, "skuhus/acme/vasby/group/scales/tx"},
		{ScopeStation, "skuhus/acme/vasby/pack-03/group/scales/tx"},
	} {
		route, err := station.GroupTx(check.scope, "scales")
		if err != nil {
			t.Fatalf("GroupTx(%s): %v", check.scope, err)
		}
		if route != (TxRoute{Topic: check.want, Scope: check.scope, Group: "scales"}) {
			t.Errorf("GroupTx(%s) = %+v, want %s in group scales", check.scope, route, check.want)
		}
	}
	for _, scope := range []TxScope{ScopeDevice, "", "region"} {
		if _, err := station.GroupTx(scope, "scales"); err == nil || !strings.Contains(err.Error(), "scope") {
			t.Errorf("GroupTx(%q): err = %v, want the scope refused", scope, err)
		}
	}
	device, _ := station.Device("scale-1")
	if route := device.TxRoute(); route != (TxRoute{Topic: "skuhus/acme/vasby/pack-03/scale-1/tx", Scope: ScopeDevice}) {
		t.Errorf("TxRoute = %+v, want the device's own tx topic with no group", route)
	}
}

// A station called "group" would have, as its device scale-1's tx topic, the
// site's group topic for a group called scale-1.
func TestStationGroupIsReserved(t *testing.T) {
	_, err := NewStationTopics("acme", "vasby", GroupLevel)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("station %q: err = %v, want it refused as reserved", GroupLevel, err)
	}
}

// v1 accepted an instance such as "Pack_03.a" because it was only a client id
// (internal/config/validate.go). It is a topic level now, so it is refused.
func TestInstanceAcceptedByV1IsRefused(t *testing.T) {
	if _, err := mustStation(t).Agent("Pack_03.a"); err == nil {
		t.Fatal("instance Pack_03.a accepted, want it refused as a topic level")
	}
}

func TestDeviceIDAgentIsReserved(t *testing.T) {
	_, err := mustStation(t).Device(AgentLevel)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("device id %q: err = %v, want it refused as reserved", AgentLevel, err)
	}
}

// matches applies MQTT filter matching: "+" is one level, "#" the rest.
func matches(filter, topic string) bool {
	filterLevels, topicLevels := strings.Split(filter, "/"), strings.Split(topic, "/")
	for index, level := range filterLevels {
		if level == "#" {
			return true
		}
		if index >= len(topicLevels) || (level != "+" && level != topicLevels[index]) {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

// Each consumer filter takes exactly its own kind of topic. In particular the
// device status filter must not take an agent's status, which is why the
// agent's topics are one level deeper and "agent" cannot be a device id.
func TestFiltersSelectTheirOwnTopics(t *testing.T) {
	station := mustStation(t)
	agent, _ := station.Agent("pack-03")
	first, _ := station.Device("scanner-1")
	second, _ := station.Device("scale-1")
	stationGroup, _ := station.GroupTx(ScopeStation, "scales")
	siteGroup, _ := station.GroupTx(ScopeSite, "scales")
	projectGroup, _ := station.GroupTx(ScopeProject, "scales")
	topics := map[string]string{
		"agent status":     agent.Status(),
		"scanner-1 rx":     first.Rx(),
		"scanner-1 tx":     first.Tx(),
		"scanner-1 status": first.Status(),
		"scale-1 rx":       second.Rx(),
		"scale-1 tx":       second.Tx(),
		"scale-1 status":   second.Status(),
		"station group tx": stationGroup.Topic,
		"site group tx":    siteGroup.Topic,
		"project group tx": projectGroup.Topic,
	}
	want := map[string][]string{
		station.EveryDeviceRx():     {"scanner-1 rx", "scale-1 rx"},
		station.EveryDeviceStatus(): {"scanner-1 status", "scale-1 status"},
		station.EveryAgentStatus():  {"agent status"},
	}
	for filter, names := range want {
		expected := map[string]bool{}
		for _, name := range names {
			expected[name] = true
		}
		for name, topic := range topics {
			if got := matches(filter, topic); got != expected[name] {
				t.Errorf("%s matches %s (%s) = %t, want %t", filter, name, topic, got, expected[name])
			}
		}
	}
}

// The matcher itself, against cases whose answer does not depend on it.
func TestMatcher(t *testing.T) {
	for _, check := range []struct {
		filter, topic string
		want          bool
	}{
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/x/c", false},
		{"a/+", "a/b/c", false},
		{"a/#", "a/b/c", true},
		{"a/b", "a/b", true},
		{"a/b", "a/b/c", false},
	} {
		if got := matches(check.filter, check.topic); got != check.want {
			t.Errorf("matches(%q, %q) = %t, want %t", check.filter, check.topic, got, check.want)
		}
	}
}
