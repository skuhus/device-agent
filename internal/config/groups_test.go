package config

import (
	"slices"
	"strings"
	"testing"
)

const groupsConfig = `
    broadcast_groups:
      site: [scales]
      station: [front, scales]
`

// A device's broadcast groups are read by scope, and a scope left out has
// none. The same name at two scopes is two groups, and is accepted.
func TestBroadcastGroupsLoad(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, "    device_type: honeywell-1470g\n",
		"    device_type: honeywell-1470g"+groupsConfig, 1))
	cfg, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	groups := cfg.Devices[0].BroadcastGroups
	if len(groups.Project) != 0 || !slices.Equal(groups.Site, []string{"scales"}) || !slices.Equal(groups.Station, []string{"front", "scales"}) {
		t.Errorf("broadcast_groups = %+v, want none at the project, scales at the site, front and scales at the station", groups)
	}
}

// Loading is strict inside broadcast_groups as everywhere: a misspelt scope,
// or a list where the scopes go, is refused with its line.
func TestBroadcastGroupsKeyIsStrict(t *testing.T) {
	for name, testCase := range map[string]struct{ body, line string }{
		"a misspelt scope":           {"\n    broadcast_groups:\n      sites: [scales]\n", "line 27: field sites not found"},
		"a list in place of a scope": {"\n    broadcast_groups: [scales]\n", "line 26: cannot unmarshal !!seq"},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newFixture(t, strings.Replace(validConfig, "    device_type: honeywell-1470g\n",
				"    device_type: honeywell-1470g"+testCase.body, 1))
			_, _, err := load(t, fixture.path, noEnv(), Overrides{})
			if err == nil || !strings.Contains(err.Error(), testCase.line) {
				t.Fatalf("err = %v, want a refusal with %q", err, testCase.line)
			}
		})
	}
}

// A station called "group" would have the site's broadcast group topics as its
// device tx topics.
func TestStationGroupIsReserved(t *testing.T) {
	problems := validateIdentity(Identity{Project: "acme", Site: "vasby", Station: reservedStationID})
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), `identity.station "group" is reserved`) {
		t.Errorf("problems = %v, want the station refused as reserved", problems)
	}
	if problems := validateIdentity(Identity{Project: "group", Site: "group", Station: "pack-03"}); len(problems) != 0 {
		t.Errorf("project and site group: problems = %v, want them accepted; only the station is reserved", problems)
	}
}
