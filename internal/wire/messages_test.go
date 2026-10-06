package wire

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func decode(t *testing.T, message any) map[string]any {
	t.Helper()
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return fields
}

var at = time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)

func testBuilder() *Builder {
	return NewBuilder(Agent{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: "pack-03", AgentVersion: "2.0.0"},
		func() string { return "id" })
}

// everyTxResult builds one result per code, through its constructor.
func everyTxResult(builder *Builder, device Device) []TxResult {
	tx := TxRef{ID: "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b", Sender: "label-service"}
	return []TxResult{
		builder.TxAccepted(device, true, tx, at),
		builder.TxWritten(device, true, tx, 120, 0, at),
		builder.TxAlreadyWritten(device, true, tx, at.Add(-time.Minute), at),
		builder.TxInProgress(device, true, tx, TxWriting, at.Add(-time.Minute), 40, at),
		builder.TxInvalidMessage(device, true, TxRef{}, "unexpected end of JSON input", at),
		builder.TxInvalidID(device, true, TxRef{ID: "not-a-uuid", Sender: "label-service"}, at),
		builder.TxPortUnavailable(device, false, tx, ErrorBusy, "device or resource busy", 3, at),
		builder.TxWriteFailed(device, false, tx, ErrorDisconnected, "input/output error", 40, 0, at),
		builder.TxExpired(device, false, tx, 2, at),
		builder.TxAgentStopping(device, true, tx, 1024, at),
	}
}

// A frame that is not UTF-8 travels whole in raw_b64 and gets no text, rather
// than a lossy rendering a consumer might trust.
func TestRxBinaryFrame(t *testing.T) {
	frame := []byte{0x02, 0xff, 0xfe, 0x03}
	fields := decode(t, testBuilder().Rx(Device{ID: "scale-1"}, 7, frame, at))
	if fields["text"] != nil || fields["text_valid"] != false {
		t.Errorf("text = %v, text_valid = %v, want null and false", fields["text"], fields["text_valid"])
	}
	raw, err := base64.StdEncoding.DecodeString(fields["raw_b64"].(string))
	if err != nil || string(raw) != string(frame) {
		t.Errorf("raw_b64 decodes to %x (%v), want %x", raw, err, frame)
	}
}

// An empty device_type is "none configured", published as null, not as "".
func TestDeviceTypeNullWhenNotConfigured(t *testing.T) {
	builder := testBuilder()
	device := Device{ID: "scanner-1"}
	for name, message := range map[string]any{
		"rx":        builder.Rx(device, 1, []byte("x"), at),
		"event":     builder.PortOpened(device, "/dev/ttyACM0", at),
		"tx_result": builder.TxAccepted(device, true, TxRef{ID: "t", Sender: "s"}, at),
	} {
		fields := decode(t, message)
		if value, present := fields["device_type"]; !present || value != nil {
			t.Errorf("%s: device_type = %v (present %t), want null", name, value, present)
		}
	}
	entry := decode(t, builder.Keepalive(at, at, 15*time.Second, 3, []DeviceState{{Device: device}}))["devices"].([]any)[0].(map[string]any)
	if value, present := entry["device_type"]; !present || value != nil {
		t.Errorf("keepalive device: device_type = %v (present %t), want null", value, present)
	}
}

// Consumers compare timestamps across stations in different zones.
func TestTimestampsAreUTC(t *testing.T) {
	local := time.Date(2026, 9, 29, 13, 0, 0, 0, time.FixedZone("UTC+5", 5*3600))
	builder := testBuilder()
	if got := decode(t, builder.Offline(OfflineShutdown, local))["agent_ts"]; got != "2026-09-29T08:00:00.000Z" {
		t.Errorf("agent_ts = %v, want the UTC rendering", got)
	}
	detail := decode(t, builder.TxInProgress(Device{ID: "printer-1"}, true, TxRef{ID: "t", Sender: "s"}, TxQueued, local, 0, local))["detail"].(map[string]any)
	if detail["since"] != "2026-09-29T08:00:00.000Z" {
		t.Errorf("in_progress since = %v, want the UTC rendering", detail["since"])
	}
}

// Every constructor gives an object, so a consumer never has to handle a null
// detail.
func TestDetailIsNeverNull(t *testing.T) {
	builder := testBuilder()
	device := Device{ID: "scanner-1"}
	for _, event := range []Event{
		builder.PortOpened(device, "/dev/ttyACM0", at),
		builder.PortClosed(device, "/dev/ttyACM0", at),
		builder.PortLost(device, "/dev/ttyACM0", ErrorDisconnected, "read: input/output error", at),
		builder.PortOpenFailed(device, "/dev/ttyACM0", ErrorAbsent, "no such file or directory", at),
		builder.BytesDiscarded(device, DiscardOversize, 300, at),
	} {
		if _, isObject := decode(t, event)["detail"].(map[string]any); !isObject {
			t.Errorf("event %s: detail is not an object", event.Code)
		}
	}
	for _, result := range everyTxResult(builder, Device{ID: "printer-1"}) {
		if _, isObject := decode(t, result)["detail"].(map[string]any); !isObject {
			t.Errorf("tx result %s: detail is not an object", result.Code)
		}
	}
}

// Each constructor reports the state its code has, every code has one, and no
// two codes read the same to a person.
func TestTxResultConstructors(t *testing.T) {
	results := everyTxResult(testBuilder(), Device{ID: "printer-1"})
	if len(results) != len(txCodeStates) {
		t.Fatalf("%d constructors tested, %d codes defined", len(results), len(txCodeStates))
	}
	texts := map[string]TxCode{}
	for _, result := range results {
		want, known := txCodeStates[result.Code]
		if !known || result.State != want || want == "" {
			t.Errorf("%s: state %q, want %q (known %t)", result.Code, result.State, want, known)
		}
		if other, seen := texts[result.Text]; seen || result.Text == "" {
			t.Errorf("%s: text %q empty or shared with %s", result.Code, result.Text, other)
		}
		texts[result.Text] = result.Code
	}
}

// A tx that could not be read has no id or sender to copy; they are null, not
// empty strings a sender might match against.
func TestTxResultForUnreadableTx(t *testing.T) {
	fields := decode(t, testBuilder().TxInvalidMessage(Device{ID: "printer-1"}, true, TxRef{},
		"invalid character 'x' looking for beginning of value", at))
	if fields["tx_id"] != nil || fields["sender"] != nil {
		t.Errorf("tx_id, sender = %v, %v, want null", fields["tx_id"], fields["sender"])
	}
	if fields["state"] != "failed" {
		t.Errorf("state = %v, want failed", fields["state"])
	}
	want := "not a valid tx: invalid character 'x' looking for beginning of value"
	if fields["text"] != want {
		t.Errorf("text = %q, want %q", fields["text"], want)
	}
}

// A resend of a tx still in progress is how a sender asks where it stands
// (#11 Q5, Q6), so the answer says which stage and since when.
func TestTxInProgressSaysWhereTheTxStands(t *testing.T) {
	builder := testBuilder()
	tx := TxRef{ID: "t", Sender: "label-service"}
	since := at.Add(-90 * time.Second)
	queued := builder.TxInProgress(Device{ID: "printer-1"}, true, tx, TxQueued, since, 0, at)
	writing := builder.TxInProgress(Device{ID: "printer-1"}, true, tx, TxWriting, since, 4096, at)
	for _, check := range []struct {
		result TxResult
		text   string
		stage  string
	}{
		{queued, "a tx with this id is queued for the port since 2026-09-29T07:58:30.000Z; this one was not taken", "queued"},
		{writing, "a tx with this id is being written since 2026-09-29T07:58:30.000Z, 4096 bytes so far; this one was not taken", "writing"},
	} {
		fields := decode(t, check.result)
		if fields["state"] != "rejected" || fields["code"] != "in_progress" {
			t.Errorf("state, code = %v, %v, want rejected, in_progress", fields["state"], fields["code"])
		}
		if fields["text"] != check.text {
			t.Errorf("text = %q, want %q", fields["text"], check.text)
		}
		if stage := fields["detail"].(map[string]any)["stage"]; stage != check.stage {
			t.Errorf("stage = %v, want %s", stage, check.stage)
		}
	}
}

// The threshold a consumer applies is published, so it follows the agent's
// configuration instead of a number each consumer hard-codes (#11 Q7).
func TestKeepaliveGoneAfter(t *testing.T) {
	for _, check := range []struct {
		interval time.Duration
		missed   int
		want     float64
	}{
		{15 * time.Second, 3, 45},
		{10 * time.Second, 5, 50},
	} {
		fields := decode(t, testBuilder().Keepalive(at, at, check.interval, check.missed, nil))
		if fields["gone_after_s"] != check.want || fields["interval_s"] != check.interval.Seconds() {
			t.Errorf("interval %s, missed %d: gone_after_s %v, interval_s %v, want %v and %v",
				check.interval, check.missed, fields["gone_after_s"], fields["interval_s"], check.want, check.interval.Seconds())
		}
	}
}

// With no devices configured the keepalive still says so with an empty list.
func TestKeepaliveWithoutDevices(t *testing.T) {
	devices, isList := decode(t, testBuilder().Keepalive(at, at, 15*time.Second, 3, nil))["devices"].([]any)
	if !isList || len(devices) != 0 {
		t.Errorf("devices = %v, want []", devices)
	}
}

// The default ids are UUID version 4, as DESIGN-V2.md says the agent's own ids
// are, and differ between messages.
func TestDefaultIDs(t *testing.T) {
	builder := NewBuilder(Agent{}, nil)
	first, second := builder.Offline(OfflineShutdown, at).ID, builder.Offline(OfflineShutdown, at).ID
	for _, id := range []string{first, second} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.Version() != 4 {
			t.Errorf("id %q: version %v, err %v, want a version 4 UUID", id, parsed.Version(), err)
		}
	}
	if first == second {
		t.Errorf("two messages got the same id %q", first)
	}
}

// readTxCase is one payload a sender might publish, what ReadTx makes of it,
// and, where the tx schema in protocol/ cannot agree, why.
type readTxCase struct {
	name, payload string
	code          TxCode
	wantRef       TxRef
	schemaDiffers string
}

// readTxCases are the tx payloads TestReadTx and TestTxSchemaAgreesWithReadTx
// both run.
func readTxCases() []readTxCase {
	const id = "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b"
	good := `{"schema":2,"id":"` + id + `","sender":"label-service","raw_b64":"XlhBXkZEU0tVLTEwNDJeRlNeWFo="}`
	return []readTxCase{
		{name: "good", payload: good, wantRef: TxRef{ID: id, Sender: "label-service"}},
		{name: "upper-case id", payload: strings.Replace(good, id, strings.ToUpper(id), 1),
			wantRef: TxRef{ID: strings.ToUpper(id), Sender: "label-service"}},
		{name: "not JSON", payload: `^XA^FD`, code: TxCodeInvalidMessage},
		{name: "an array", payload: `[1]`, code: TxCodeInvalidMessage},
		{name: "null", payload: `null`, code: TxCodeInvalidMessage},
		{name: "schema as a string", payload: strings.Replace(good, `"schema":2`, `"schema":"2"`, 1), code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "label-service"}},
		{name: "two values", payload: good + good, code: TxCodeInvalidMessage},
		{name: "unknown field", payload: strings.Replace(good, `"schema":2`, `"schema":2,"qos":1`, 1), code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "label-service"}},
		{name: "field names in another case", payload: `{"SCHEMA":2,"ID":"` + id + `","Sender":"label-service","Raw_B64":"XlhB"}`,
			code: TxCodeInvalidMessage},
		{name: "one name in another case", payload: `{"schema":2,"ID":"` + id + `","sender":"s","raw_b64":"AA=="}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{Sender: "s"}},
		{name: "a name that only Unicode folding matches", payload: `{"schema":2,"id":"` + id + `","\u017fender":"s","raw_b64":"AA=="}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id}},
		{name: "a name twice, in two cases", payload: `{"schema":2,"id":"` + id + `","Id":"` + id + `","sender":"s","raw_b64":"AA=="}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "missing raw_b64", payload: `{"schema":2,"id":"` + id + `","sender":"s"}`, code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "null id", payload: `{"schema":2,"id":null,"sender":"s","raw_b64":"AA=="}`, code: TxCodeInvalidMessage,
			wantRef: TxRef{Sender: "s"}},
		{name: "null id, spaced", payload: "{\"schema\": 2, \"id\" :\n null , \"sender\": \"s\", \"raw_b64\": \"AA==\"}",
			code: TxCodeInvalidMessage, wantRef: TxRef{Sender: "s"}},
		{name: "schema 1", payload: strings.Replace(good, `"schema":2`, `"schema":1`, 1), code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "label-service"}},
		{name: "schema 2.0", payload: strings.Replace(good, `"schema":2`, `"schema":2.0`, 1), code: TxCodeInvalidMessage,
			wantRef:       TxRef{ID: id, Sender: "label-service"},
			schemaDiffers: "JSON Schema compares numbers by value, so 2.0 is the integer 2 to it"},
		{name: "empty sender", payload: strings.Replace(good, `"label-service"`, `""`, 1), code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id}},
		{name: "unpadded base64", payload: strings.Replace(good, `XlhBXkZEU0tVLTEwNDJeRlNeWFo=`, `XlhBXkZEU0tVLTEwNDJeRlNeWFo`, 1),
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id, Sender: "label-service"}},
		{name: "url base64", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":"_-8="}`, code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "non-zero pad bits", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":"AB=="}`, code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "line feed in base64", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":"AA\n=="}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "carriage return in base64", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":"AA\r=="}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "base64 wrapped at the end", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":"AA==\r\n"}`,
			code: TxCodeInvalidMessage, wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "no bytes", payload: `{"schema":2,"id":"` + id + `","sender":"s","raw_b64":""}`, code: TxCodeInvalidMessage,
			wantRef: TxRef{ID: id, Sender: "s"}},
		{name: "id not a UUID", payload: strings.Replace(good, id, "job-1042", 1), code: TxCodeInvalidID,
			wantRef: TxRef{ID: "job-1042", Sender: "label-service"}},
		{name: "id in braces", payload: strings.Replace(good, id, "{"+id+"}", 1), code: TxCodeInvalidID,
			wantRef: TxRef{ID: "{" + id + "}", Sender: "label-service"}},
		{name: "id as bare hex", payload: strings.Replace(good, id, strings.ReplaceAll(id, "-", ""), 1), code: TxCodeInvalidID,
			wantRef: TxRef{ID: strings.ReplaceAll(id, "-", ""), Sender: "label-service"}},
	}
}

// Every way a tx can be wrong gets its own failed result, with the id and the
// sender wherever they could be read, and a good one comes back as its bytes.
func TestReadTx(t *testing.T) {
	for _, testCase := range readTxCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ref, raw, problem := ReadTx([]byte(testCase.payload))
			if ref != testCase.wantRef {
				t.Errorf("ref = %+v, want %+v", ref, testCase.wantRef)
			}
			if testCase.code == "" {
				if problem != nil {
					t.Fatalf("rejected: %+v", problem)
				}
				if len(raw) == 0 {
					t.Errorf("no bytes for a tx that was taken")
				}
				return
			}
			if problem == nil {
				t.Fatalf("accepted, with %q", raw)
			}
			if problem.Code != testCase.code || problem.Text == "" {
				t.Errorf("problem = %+v, want code %s with a text", problem, testCase.code)
			}
			if raw != nil {
				t.Errorf("raw = %q for a tx that cannot be taken", raw)
			}
		})
	}
}

// A field name in another case is refused by name, every one of them, so that
// the sender sees which; encoding/json alone would take each for its field
// (#47).
func TestReadTxNamesEveryUnknownField(t *testing.T) {
	const id = "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b"
	for _, check := range []struct {
		payload, want string
	}{
		{`{"schema":2,"ID":"` + id + `","sender":"s","raw_b64":"AA=="}`,
			`unknown field "ID"; a tx has exactly schema, id, sender and raw_b64`},
		{`{"SCHEMA":2,"ID":"` + id + `","Sender":"s","Raw_B64":"AA=="}`,
			`unknown fields "ID", "Raw_B64", "SCHEMA", "Sender"; a tx has exactly schema, id, sender and raw_b64`},
	} {
		_, _, problem := ReadTx([]byte(check.payload))
		if problem == nil || problem.Code != TxCodeInvalidMessage || problem.Text != check.want {
			t.Errorf("%s: problem = %+v, want invalid_message: %s", check.payload, problem, check.want)
		}
	}
}

// A good tx comes back as the bytes it carries.
func TestReadTxDecodesTheBytes(t *testing.T) {
	_, raw, problem := ReadTx([]byte(readTxCases()[0].payload))
	if problem != nil || string(raw) != "^XA^FDSKU-1042^FS^XZ" {
		t.Errorf("raw = %q, problem %+v; want the decoded bytes", raw, problem)
	}
}
