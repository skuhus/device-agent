package wire

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// These tests hold DESIGN-V2.md, "Message formats", and this package to each
// other. Every JSON example in the section is what the code builds from the
// inputs below, value for value, and the code tables list exactly the codes the
// package defines. A field added, removed or renamed on one side only fails.

const designPath = "../../DESIGN-V2.md"

var (
	exampleAt  = time.Date(2026, 9, 29, 8, 0, 0, 123_000_000, time.UTC)
	exampleOf  = Agent{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: "pack-03", AgentVersion: "2.0.0"}
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

// examples builds each message the section shows, keyed by the name after
// "```json" on the example's opening fence.
func examples() map[string]any {
	exampleStation, _ := NewStationTopics(exampleOf.Project, exampleOf.Site, exampleOf.Station)
	scannerTopics, _ := exampleStation.Device(scanner.ID)
	printerTopics, _ := exampleStation.Device(printer.ID)
	printers, _ := exampleStation.GroupTx(ScopeSite, "printers")
	granted := SubscribeAnswer{Code: 1, Answered: true}
	return map[string]any{
		"rx": builderWithID("5b7b4f6e-2f0a-4c1e-9d3a-8f6e1c2b7a90").
			Rx(scanner, 1042, []byte("7310425012345"), exampleAt),
		"event": builderWithID("c3a1e2d4-5f60-4b7a-8c9d-0e1f2a3b4c5d").
			PortLost(scanner, scannerTTY, ErrorDisconnected, "read "+scannerTTY+": input/output error", exampleAt),
		"tx": Tx{
			Schema: Schema,
			ID:     exampleTx,
			Sender: "label-service",
			RawB64: base64.StdEncoding.EncodeToString([]byte("^XA^FDSKU-1042^FS^XZ")),
		},
		"tx_result": builderWithID("7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e").
			TxPortUnavailable(printer, false, TxRef{ID: exampleTx, Sender: "label-service"},
				ErrorBusy, "open "+printerTTY+": device or resource busy", 3, exampleAt),
		"tx_result_in_progress": builderWithID("2d3c4b5a-6978-4e5f-8a1b-0c9d8e7f6a5b").
			TxInProgress(printer, true, TxRef{ID: exampleTx, Sender: "label-service"},
				TxWriting, exampleAt.Add(-90*time.Second), 61440, exampleAt),
		"keepalive": builderWithID("9f8e7d6c-5b4a-4392-8170-6f5e4d3c2b1a").
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
		"offline": builderWithID("1a2b3c4d-5e6f-4a0b-9c8d-7e6f5a4b3c2d").
			Offline(OfflineWill, exampleAt),
	}
}

type design struct {
	examples map[string]string
	lines    []string
}

func readDesign(t *testing.T) design {
	t.Helper()
	file, err := os.Open(designPath)
	if err != nil {
		t.Fatalf("open %s: %v", designPath, err)
	}
	defer file.Close()
	doc := design{examples: map[string]string{}}
	var name string
	var body strings.Builder
	inExample := false
	reader := bufio.NewScanner(file)
	for reader.Scan() {
		line := reader.Text()
		doc.lines = append(doc.lines, line)
		switch {
		case !inExample && strings.HasPrefix(line, "```json "):
			name, inExample = strings.TrimPrefix(line, "```json "), true
			body.Reset()
		case inExample && line == "```":
			if _, duplicate := doc.examples[name]; duplicate {
				t.Fatalf("%s has two examples named %q", designPath, name)
			}
			doc.examples[name], inExample = body.String(), false
		case inExample:
			body.WriteString(line + "\n")
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read %s: %v", designPath, err)
	}
	return doc
}

func TestDesignExamplesAreWhatTheCodeBuilds(t *testing.T) {
	doc := readDesign(t)
	built := examples()
	for name := range doc.examples {
		if _, known := built[name]; !known {
			t.Errorf("%s has an example %q that no test builds", designPath, name)
		}
	}
	for name, message := range built {
		text, found := doc.examples[name]
		if !found {
			t.Errorf("%s has no example %q (a fence opening with \"```json %s\")", designPath, name, name)
			continue
		}
		var documented any
		if err := json.Unmarshal([]byte(text), &documented); err != nil {
			t.Errorf("example %q is not valid JSON: %v", name, err)
			continue
		}
		body, err := json.MarshalIndent(message, "", "  ")
		if err != nil {
			t.Fatalf("marshal %q: %v", name, err)
		}
		var produced any
		if err := json.Unmarshal(body, &produced); err != nil {
			t.Fatalf("unmarshal %q: %v", name, err)
		}
		if !reflect.DeepEqual(documented, produced) {
			t.Errorf("example %q differs from what the code builds.\ndocumented:\n%s\nbuilt:\n%s", name, text, body)
		}
	}
}

var codeCell = regexp.MustCompile("`([a-z_]+)`")

// tableRows returns the cells of each row of the table under heading, with the
// backticked words of each cell.
func tableRows(t *testing.T, doc design, heading string) [][][]string {
	t.Helper()
	start := -1
	for index, line := range doc.lines {
		if line == heading {
			start = index
			break
		}
	}
	if start < 0 {
		t.Fatalf("%s has no heading %q", designPath, heading)
	}
	var rows [][][]string
	for _, line := range doc.lines[start+1:] {
		if strings.HasPrefix(line, "#") {
			break
		}
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		var cells [][]string
		for _, cell := range strings.Split(strings.Trim(line, "|"), "|") {
			var words []string
			for _, match := range codeCell.FindAllStringSubmatch(cell, -1) {
				words = append(words, match[1])
			}
			cells = append(cells, words)
		}
		rows = append(rows, cells)
	}
	if len(rows) == 0 {
		t.Fatalf("the table under %q has no rows", heading)
	}
	return rows
}

func TestDesignEventCodesMatchTheCode(t *testing.T) {
	builder := builderWithID("id")
	built := map[string][]string{}
	for _, event := range []Event{
		builder.PortOpened(scanner, scannerTTY, exampleAt),
		builder.PortClosed(scanner, scannerTTY, exampleAt),
		builder.PortLost(scanner, scannerTTY, ErrorDisconnected, "e", exampleAt),
		builder.PortOpenFailed(scanner, scannerTTY, ErrorAbsent, "e", exampleAt),
		builder.BytesDiscarded(scanner, DiscardOversize, 1, exampleAt),
	} {
		built[string(event.Code)] = strings.Split(sortedKeys(event.Detail), ",")
	}
	documented := map[string][]string{}
	for _, row := range tableRows(t, readDesign(t), "#### Event codes") {
		if len(row) < 2 || len(row[0]) != 1 {
			t.Fatalf("event code row %v: want a code, then its detail keys", row)
		}
		keys := append([]string{}, row[1]...)
		sort.Strings(keys)
		documented[row[0][0]] = keys
	}
	if !reflect.DeepEqual(documented, built) {
		t.Errorf("event codes and detail keys differ.\ndocumented: %v\nbuilt:      %v", documented, built)
	}
}

func TestDesignTxCodesMatchTheCode(t *testing.T) {
	built := map[string]string{}
	for _, result := range everyTxResult(builderWithID("id"), printer) {
		built[string(result.Code)] = string(result.State) + " " + sortedKeys(result.Detail)
	}
	documented := map[string]string{}
	for _, row := range tableRows(t, readDesign(t), "#### Tx result codes") {
		if len(row) < 3 || len(row[0]) != 1 || len(row[1]) != 1 {
			t.Fatalf("tx code row %v: want a state, a code, then its detail keys", row)
		}
		keys := append([]string{}, row[2]...)
		sort.Strings(keys)
		documented[row[1][0]] = row[0][0] + " " + strings.Join(keys, ",")
	}
	if !reflect.DeepEqual(documented, built) {
		t.Errorf("tx result codes, states and detail keys differ.\ndocumented: %v\nbuilt:      %v", documented, built)
	}
}

func sortedKeys(detail map[string]any) string {
	keys := make([]string, 0, len(detail))
	for key := range detail {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
