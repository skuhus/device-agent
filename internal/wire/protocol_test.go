package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// These tests hold protocol/ and this package to each other. Every message the
// package builds validates against its schema in messages.schema.json, and
// against a closed copy of it that fails on a field the schema does not
// declare, on a declared field the message leaves out, and on a value the
// schema does not list. Every example in asyncapi.yaml is what the code builds
// from the inputs below, value for value, and the schema lists exactly the
// codes, values and detail keys the package defines.

const (
	asyncAPIPath = "../../protocol/asyncapi.yaml"
	schemaPath   = "../../protocol/messages.schema.json"
)

var (
	exampleAt  = time.Date(2026, 9, 29, 8, 0, 0, 123_000_000, time.UTC)
	exampleOf  = Agent{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: "pack-03", AgentVersion: "2.2.0"}
	scanner    = Device{ID: "scanner-1", Type: "symbol-05e0-1701", Expiry: 30 * time.Second}
	printer    = Device{ID: "printer-1", Type: "zebra-zt410", Expiry: 30 * time.Second}
	exampleTx  = "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b"
	scannerTTY = "/dev/serial/by-id/usb-Symbol_Technologies-if00"
	printerTTY = "/dev/serial/by-id/usb-Zebra_ZT410-if00"
)

// builderWithID returns a builder whose every message gets id.
func builderWithID(id string) *Builder {
	return NewBuilder(exampleOf, func() string { return id })
}

// examples builds each example asyncapi.yaml shows, keyed by its message's
// name in components.messages and the example's name, as "message/example".
func examples() map[string]any {
	exampleStation, _ := NewStationTopics(exampleOf.Project, exampleOf.Site, exampleOf.Station)
	scannerTopics, _ := exampleStation.Device(scanner.ID)
	printerTopics, _ := exampleStation.Device(printer.ID)
	printers, _ := exampleStation.GroupTx(ScopeSite, "printers")
	granted := SubscribeAnswer{Code: 1, Answered: true}
	return map[string]any{
		"rx/scan": builderWithID("5b7b4f6e-2f0a-4c1e-9d3a-8f6e1c2b7a90").
			Rx(scanner, 1042, []byte("7310425012345"), exampleAt),
		"event/port_lost": builderWithID("c3a1e2d4-5f60-4b7a-8c9d-0e1f2a3b4c5d").
			PortLost(scanner, scannerTTY, ErrorDisconnected, "read "+scannerTTY+": input/output error", exampleAt),
		"tx/label": Tx{
			Schema: Schema,
			ID:     exampleTx,
			Sender: "label-service",
			RawB64: base64.StdEncoding.EncodeToString([]byte("^XA^FDSKU-1042^FS^XZ")),
		},
		"tx_result/port_unavailable": builderWithID("7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e").
			TxPortUnavailable(printer, false, TxRef{ID: exampleTx, Sender: "label-service"},
				ErrorBusy, "open "+printerTTY+": device or resource busy", 3, exampleAt),
		"tx_result/in_progress": builderWithID("2d3c4b5a-6978-4e5f-8a1b-0c9d8e7f6a5b").
			TxInProgress(printer, true, TxRef{ID: exampleTx, Sender: "label-service"},
				TxWriting, exampleAt.Add(-90*time.Second), 61440, exampleAt),
		"keepalive/scanner_and_printer": builderWithID("9f8e7d6c-5b4a-4392-8170-6f5e4d3c2b1a").
			Keepalive(exampleAt.Add(-time.Hour), exampleAt, 15*time.Second, 3, []DeviceState{
				{Device: scanner, Open: true, Counters: DeviceCounters{
					RxFrames: 1042, RxBytes: 15656,
					Discards:    DiscardCounts{InterCharTimeout: 2},
					BufferDepth: 0,
				}, TxRoutes: []TxRouteState{{Route: scannerTopics.TxRoute(), Answer: granted}}},
				{Device: printer, Open: false, Counters: DeviceCounters{
					FailedOpens: OpenFailureCounts{Busy: 3},
					TxFailed:    1,
				}, TxRoutes: []TxRouteState{{Route: printerTopics.TxRoute(), Answer: granted}, {Route: printers, Answer: granted}}},
			}),
		"offline/will": builderWithID("1a2b3c4d-5e6f-4a0b-9c8d-7e6f5a4b3c2d").
			Offline(OfflineWill, exampleAt),
	}
}

// builtMessage is one message the package builds, with the schema definition
// it is published under.
type builtMessage struct {
	name, definition string
	message          any
}

// everyMessage builds every message the package can publish: each event code,
// each tx result code, rx with and without text, keepalives with and without
// devices and with every scope and an unanswered subscription, both offline
// reasons, and messages about a device with no device_type.
func everyMessage(t *testing.T) []builtMessage {
	t.Helper()
	builder := builderWithID("0f1e2d3c-4b5a-4968-8776-655443322110")
	untyped := Device{ID: "scale-1", Expiry: 30 * time.Second}
	station, err := NewStationTopics("acme", "vasby", "pack-03")
	if err != nil {
		t.Fatal(err)
	}
	scaleTopics, err := station.Device(untyped.ID)
	if err != nil {
		t.Fatal(err)
	}
	routes := []TxRouteState{{Route: scaleTopics.TxRoute(), Answer: SubscribeAnswer{Code: 1, Answered: true}}}
	for _, scope := range []TxScope{ScopeProject, ScopeSite, ScopeStation} {
		route, err := station.GroupTx(scope, "scales")
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, TxRouteState{Route: route, Answer: SubscribeAnswer{Code: 0x87, Answered: scope != ScopeStation}})
	}
	messages := []builtMessage{
		{"rx with text", "rx", builder.Rx(scanner, 1, []byte("7310425012345"), at)},
		{"rx without text", "rx", builder.Rx(untyped, 2, []byte{0x02, 0xff, 0xfe, 0x03}, at)},
		{"tx", "tx", examples()["tx/label"]},
		{"keepalive without devices", "keepalive", builder.Keepalive(at, at, 15*time.Second, 3, nil)},
		{"keepalive with every scope", "keepalive", builder.Keepalive(at.Add(-time.Minute), at, 15*time.Second, 3,
			[]DeviceState{{Device: untyped, Open: true, TxRoutes: routes}})},
		{"keepalive with no tx topic", "keepalive", builder.Keepalive(at, at, 15*time.Second, 3, []DeviceState{{Device: untyped}})},
		{"offline shutdown", "offline", builder.Offline(OfflineShutdown, at)},
		{"offline will", "offline", builder.Offline(OfflineWill, at)},
	}
	for _, device := range []Device{scanner, untyped} {
		for _, event := range []Event{
			builder.PortOpened(device, scannerTTY, at),
			builder.PortClosed(device, scannerTTY, at),
			builder.PortLost(device, scannerTTY, ErrorDisconnected, "read: input/output error", at),
			builder.PortOpenFailed(device, scannerTTY, ErrorAbsent, "no such file or directory", at),
			builder.BytesDiscarded(device, DiscardOversize, 300, at),
		} {
			messages = append(messages, builtMessage{fmt.Sprintf("event %s, %s", event.Code, device.ID), "event", event})
		}
		for _, result := range everyTxResult(builder, device) {
			messages = append(messages, builtMessage{fmt.Sprintf("tx_result %s, %s", result.Code, device.ID), "tx_result", result})
		}
	}
	for name, message := range examples() {
		definition, _, _ := strings.Cut(name, "/")
		messages = append(messages, builtMessage{"example " + name, definition, message})
	}
	return messages
}

// readSchema reads messages.schema.json as the validator reads JSON, with
// numbers as json.Number.
func readSchema(t *testing.T) map[string]any {
	t.Helper()
	file, err := os.Open(schemaPath)
	if err != nil {
		t.Fatalf("open %s: %v", schemaPath, err)
	}
	defer file.Close()
	doc, err := jsonschema.UnmarshalJSON(file)
	if err != nil {
		t.Fatalf("read %s: %v", schemaPath, err)
	}
	return doc.(map[string]any)
}

// closed returns a copy of a schema that holds a message to exactly what it
// declares: every object that lists its properties takes no other and
// requires them all, and every x-extensible-enum is an enum. The published
// schema is open in both ways, so that a consumer reads a later minor version
// (#46 Q3); the agent's own messages must still be exactly what it declares.
func closed(node any) any {
	switch value := node.(type) {
	case map[string]any:
		copied := make(map[string]any, len(value))
		for key, child := range value {
			copied[key] = closed(child)
		}
		if properties, listed := value["properties"].(map[string]any); listed && value["type"] == "object" {
			names := make([]any, 0, len(properties))
			for _, name := range sortedNames(properties) {
				names = append(names, name)
			}
			copied["additionalProperties"] = false
			copied["required"] = names
		}
		if values, extensible := value["x-extensible-enum"]; extensible {
			copied["enum"] = closed(values)
		}
		return copied
	case []any:
		copied := make([]any, len(value))
		for index, child := range value {
			copied[index] = closed(child)
		}
		return copied
	default:
		return value
	}
}

// schemas compiles one definition of schema at a time.
type schemas struct {
	compiler *jsonschema.Compiler
}

func newSchemas(t *testing.T, doc any) schemas {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaPath, doc); err != nil {
		t.Fatalf("add %s: %v", schemaPath, err)
	}
	return schemas{compiler: compiler}
}

func (set schemas) definition(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	compiled, err := set.compiler.Compile(schemaPath + "#/definitions/" + name)
	if err != nil {
		t.Fatalf("compile the definition %q: %v", name, err)
	}
	return compiled
}

// asJSON is message as a consumer receives it: marshalled, and read back the
// way the validator reads JSON.
func asJSON(t *testing.T, message any) (any, []byte) {
	t.Helper()
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("read back %s: %v", body, err)
	}
	return instance, body
}

func TestEveryMessageMatchesItsSchema(t *testing.T) {
	doc := readSchema(t)
	published, exact := newSchemas(t, doc), newSchemas(t, closed(doc))
	for _, built := range everyMessage(t) {
		instance, body := asJSON(t, built.message)
		if err := published.definition(t, built.definition).Validate(instance); err != nil {
			t.Errorf("%s: not valid against %s: %v\n%s", built.name, built.definition, err, body)
		}
		if err := exact.definition(t, built.definition).Validate(instance); err != nil {
			t.Errorf("%s: not exactly what %s declares: %v\n%s", built.name, built.definition, err, body)
		}
	}
}

// The closed copy is what makes the test above fail on an undeclared field, a
// missing one and an unlisted value, so it is checked on messages that have
// each, while the published schema takes them.
func TestClosedSchemaRefusesWhatThePublishedOneTakes(t *testing.T) {
	doc := readSchema(t)
	published, exact := newSchemas(t, doc), newSchemas(t, closed(doc))
	builder := builderWithID("1a2b3c4d-5e6f-4a0b-9c8d-7e6f5a4b3c2d")
	offline := func() any { return builder.Offline(OfflineWill, exampleAt) }
	for _, check := range []struct {
		name, definition string
		build            func() any
		change           func(map[string]any)
	}{
		{"a field the schema does not declare", "offline", offline, func(fields map[string]any) { fields["host"] = "rpi118" }},
		{"a declared field left out", "keepalive", func() any { return builder.Keepalive(exampleAt, exampleAt, 15*time.Second, 3, nil) },
			func(fields map[string]any) { delete(fields, "protocol_version") }},
		{"a value the schema does not list", "offline", offline, func(fields map[string]any) { fields["reason"] = "restart" }},
		{"a detail key the code does not have", "event", func() any { return builder.BytesDiscarded(scanner, DiscardResync, 4, exampleAt) },
			func(fields map[string]any) { fields["detail"].(map[string]any)["data_b64"] = "AA==" }},
	} {
		instance, _ := asJSON(t, check.build())
		message := instance.(map[string]any)
		check.change(message)
		if err := published.definition(t, check.definition).Validate(message); err != nil {
			t.Errorf("%s: the published schema refuses it: %v", check.name, err)
		}
		if err := exact.definition(t, check.definition).Validate(message); err == nil {
			t.Errorf("%s: the closed schema takes it", check.name)
		}
	}
}

// readAsyncAPI reads asyncapi.yaml into plain maps and slices, with numbers as
// JSON would give them, so that its examples compare with built messages.
func readAsyncAPI(t *testing.T) map[string]any {
	t.Helper()
	text, err := os.ReadFile(asyncAPIPath)
	if err != nil {
		t.Fatalf("read %s: %v", asyncAPIPath, err)
	}
	var doc any
	if err := yaml.Unmarshal(text, &doc); err != nil {
		t.Fatalf("parse %s: %v", asyncAPIPath, err)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("%s as JSON: %v", asyncAPIPath, err)
	}
	var plain map[string]any
	if err := json.Unmarshal(body, &plain); err != nil {
		t.Fatalf("%s as JSON: %v", asyncAPIPath, err)
	}
	return plain
}

func TestProtocolVersionIsTheDocumentsVersion(t *testing.T) {
	info, _ := readAsyncAPI(t)["info"].(map[string]any)
	if version := info["version"]; version != ProtocolVersion {
		t.Errorf("%s has info.version %v; wire.ProtocolVersion is %s", asyncAPIPath, version, ProtocolVersion)
	}
	if major, _, _ := strings.Cut(ProtocolVersion, "."); major != strconv.Itoa(Schema) {
		t.Errorf("ProtocolVersion %s has major version %s; Schema is %d", ProtocolVersion, major, Schema)
	}
}

func TestExamplesAreWhatTheCodeBuilds(t *testing.T) {
	components, _ := readAsyncAPI(t)["components"].(map[string]any)
	messages, _ := components["messages"].(map[string]any)
	if len(messages) == 0 {
		t.Fatalf("%s has no components.messages", asyncAPIPath)
	}
	documented := map[string]any{}
	for messageName, message := range messages {
		exampleList, _ := message.(map[string]any)["examples"].([]any)
		if len(exampleList) == 0 {
			t.Errorf("%s: message %q has no example", asyncAPIPath, messageName)
		}
		for _, example := range exampleList {
			fields := example.(map[string]any)
			key := fmt.Sprintf("%s/%v", messageName, fields["name"])
			if _, duplicate := documented[key]; duplicate {
				t.Errorf("%s has two examples %q", asyncAPIPath, key)
			}
			documented[key] = fields["payload"]
		}
	}
	built := examples()
	for key := range documented {
		if _, known := built[key]; !known {
			t.Errorf("%s has an example %q that no test builds", asyncAPIPath, key)
		}
	}
	for key, message := range built {
		payload, found := documented[key]
		if !found {
			t.Errorf("%s has no example %q", asyncAPIPath, key)
			continue
		}
		body, err := json.MarshalIndent(message, "", "  ")
		if err != nil {
			t.Fatalf("marshal %q: %v", key, err)
		}
		var produced any
		if err := json.Unmarshal(body, &produced); err != nil {
			t.Fatalf("unmarshal %q: %v", key, err)
		}
		if !reflect.DeepEqual(payload, produced) {
			documentedBody, _ := json.MarshalIndent(payload, "", "  ")
			t.Errorf("example %q differs from what the code builds.\ndocumented:\n%s\nbuilt:\n%s", key, documentedBody, body)
		}
	}
}

// codeBranches returns, for each code a definition's allOf conditions name,
// the subschema that applies to it.
func codeBranches(t *testing.T, definition map[string]any) map[string]map[string]any {
	t.Helper()
	branches := map[string]map[string]any{}
	conditions, _ := definition["allOf"].([]any)
	for _, condition := range conditions {
		fields := condition.(map[string]any)
		code := nested(fields, "if", "properties", "code", "const")
		then, _ := fields["then"].(map[string]any)
		name, isString := code.(string)
		if !isString || then == nil {
			t.Fatalf("an allOf condition without an if on a code and a then: %v", fields)
		}
		branches[name] = then
	}
	return branches
}

// nested follows keys down maps, and is nil where one is missing.
func nested(node any, keys ...string) any {
	for _, key := range keys {
		fields, isMap := node.(map[string]any)
		if !isMap {
			return nil
		}
		node = fields[key]
	}
	return node
}

// detailKeys is the sorted list of detail keys a code's condition requires.
func detailKeys(then map[string]any) string {
	required, _ := nested(then, "properties", "detail", "required").([]any)
	keys := make([]string, 0, len(required))
	for _, key := range required {
		keys = append(keys, key.(string))
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

// listed is a definition's x-extensible-enum, sorted.
func listed(t *testing.T, node any) []string {
	t.Helper()
	values, _ := nested(node, "x-extensible-enum").([]any)
	if len(values) == 0 {
		t.Fatalf("no x-extensible-enum in %v", node)
	}
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, value.(string))
	}
	slices.Sort(names)
	return names
}

func TestEventCodesMatchTheSchema(t *testing.T) {
	builder := builderWithID("id")
	built := map[string]string{}
	for _, event := range []Event{
		builder.PortOpened(scanner, scannerTTY, exampleAt),
		builder.PortClosed(scanner, scannerTTY, exampleAt),
		builder.PortLost(scanner, scannerTTY, ErrorDisconnected, "e", exampleAt),
		builder.PortOpenFailed(scanner, scannerTTY, ErrorAbsent, "e", exampleAt),
		builder.BytesDiscarded(scanner, DiscardOversize, 1, exampleAt),
	} {
		built[string(event.Code)] = sortedKeys(event.Detail)
	}
	definition := nested(readSchema(t), "definitions", "event").(map[string]any)
	documented := map[string]string{}
	for code, then := range codeBranches(t, definition) {
		documented[code] = detailKeys(then)
	}
	if !reflect.DeepEqual(documented, built) {
		t.Errorf("event codes and detail keys differ.\nschema: %v\nbuilt:  %v", documented, built)
	}
	if codes := listed(t, nested(definition, "properties", "code")); !reflect.DeepEqual(codes, sortedNames(built)) {
		t.Errorf("code lists %v; the conditions and the code have %v", codes, sortedNames(built))
	}
}

func TestTxCodesMatchTheSchema(t *testing.T) {
	built := map[string]string{}
	for _, result := range everyTxResult(builderWithID("id"), printer) {
		built[string(result.Code)] = string(result.State) + " " + sortedKeys(result.Detail)
	}
	definition := nested(readSchema(t), "definitions", "tx_result").(map[string]any)
	documented := map[string]string{}
	for code, then := range codeBranches(t, definition) {
		documented[code] = fmt.Sprintf("%v %s", nested(then, "properties", "state", "const"), detailKeys(then))
	}
	if !reflect.DeepEqual(documented, built) {
		t.Errorf("tx result codes, states and detail keys differ.\nschema: %v\nbuilt:  %v", documented, built)
	}
	if codes := listed(t, nested(definition, "properties", "code")); !reflect.DeepEqual(codes, sortedNames(built)) {
		t.Errorf("code lists %v; the conditions and the code have %v", codes, sortedNames(built))
	}
}

// Every list of values in the schema is the list the package defines. The
// closed schema fails on a value the code emits and the schema does not list;
// this fails on one the schema lists and the code never emits.
func TestValueListsMatchTheCode(t *testing.T) {
	definitions := nested(readSchema(t), "definitions")
	states := map[string]bool{}
	for _, state := range txCodeStates {
		states[string(state)] = true
	}
	for _, check := range []struct {
		name   string
		schema any
		code   []string
	}{
		{"error_class", nested(definitions, "error_class"), names(ErrorClasses)},
		{"discard_reason", nested(definitions, "discard_reason"), names(DiscardReasons)},
		{"tx_result state", nested(definitions, "tx_result", "properties", "state"), sortedNames(states)},
		{"tx_topic scope", nested(definitions, "tx_topic", "properties", "scope"), names([]TxScope{ScopeDevice, ScopeProject, ScopeSite, ScopeStation})},
		{"offline reason", nested(definitions, "offline", "properties", "reason"), names([]OfflineReason{OfflineShutdown, OfflineWill})},
		{"in_progress stage", nested(codeBranches(t, nested(definitions, "tx_result").(map[string]any))["in_progress"],
			"properties", "detail", "properties", "stage"), names([]TxStage{TxQueued, TxWriting})},
		{"keepalive discards", sortedNames(nested(definitions, "keepalive_device", "properties", "discards", "properties").(map[string]any)), names(DiscardReasons)},
		{"keepalive failed_opens", sortedNames(nested(definitions, "keepalive_device", "properties", "failed_opens", "properties").(map[string]any)), names(ErrorClasses)},
	} {
		documented, isList := check.schema.([]string)
		if !isList {
			documented = listed(t, check.schema)
		}
		if !reflect.DeepEqual(documented, check.code) {
			t.Errorf("%s: the schema lists %v, the code %v", check.name, documented, check.code)
		}
	}
	for definition, kind := range map[string]string{"rx": KindRx, "event": KindEvent, "tx_result": KindTxResult, "keepalive": KindKeepalive, "offline": KindOffline} {
		if documented := nested(definitions, definition, "properties", "kind", "const"); documented != kind {
			t.Errorf("%s: kind is %v in the schema, %s in the code", definition, documented, kind)
		}
	}
}

// The tx schema refuses what ReadTx refuses and takes what it takes, case by
// case, except where a case says why the two differ.
func TestTxSchemaAgreesWithReadTx(t *testing.T) {
	txSchema := newSchemas(t, readSchema(t)).definition(t, "tx")
	for _, testCase := range readTxCases() {
		t.Run(testCase.name, func(t *testing.T) {
			// A payload that is not one JSON value is refused before validation.
			instance, err := jsonschema.UnmarshalJSON(strings.NewReader(testCase.payload))
			if err == nil {
				err = txSchema.Validate(instance)
			}
			schemaTakes, agentTakes := err == nil, testCase.code == ""
			if testCase.schemaDiffers != "" {
				if schemaTakes == agentTakes {
					t.Errorf("the schema and ReadTx agree (take it: %t), though the case says they differ: %s", agentTakes, testCase.schemaDiffers)
				}
				return
			}
			if schemaTakes != agentTakes {
				t.Errorf("the schema takes it: %t; ReadTx takes it: %t (schema: %v)", schemaTakes, agentTakes, err)
			}
		})
	}
}

func sortedKeys(detail map[string]any) string {
	return strings.Join(sortedNames(detail), ",")
}

// sortedNames is the sorted keys of a map with string keys.
func sortedNames[Value any](values map[string]Value) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// names is a list of string values, sorted.
func names[Value ~string](values []Value) []string {
	sorted := make([]string, 0, len(values))
	for _, value := range values {
		sorted = append(sorted, string(value))
	}
	slices.Sort(sorted)
	return sorted
}
