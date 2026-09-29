package wire

import (
	"encoding/base64"
	"encoding/json"
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
	for name, message := range map[string]any{
		"rx":        builder.Rx(Device{ID: "scanner-1"}, 1, []byte("x"), at),
		"event":     builder.PortOpened(Device{ID: "scanner-1"}, "/dev/ttyACM0", at),
		"tx_result": builder.TxResult(Device{ID: "scanner-1"}, true, "t", "s", TxCodeAccepted, TxOutcome{}, at),
	} {
		fields := decode(t, message)
		if value, present := fields["device_type"]; !present || value != nil {
			t.Errorf("%s: device_type = %v (present %t), want null", name, value, present)
		}
	}
	entry := decode(t, builder.Keepalive(at, at, 15*time.Second, []DeviceState{{Device: Device{ID: "scanner-1"}}}))["devices"].([]any)[0].(map[string]any)
	if value, present := entry["device_type"]; !present || value != nil {
		t.Errorf("keepalive device: device_type = %v (present %t), want null", value, present)
	}
}

// Consumers compare timestamps across stations in different zones.
func TestTimestampsAreUTC(t *testing.T) {
	local := time.Date(2026, 9, 29, 13, 0, 0, 0, time.FixedZone("UTC+5", 5*3600))
	fields := decode(t, testBuilder().Offline(OfflineShutdown, local))
	if fields["agent_ts"] != "2026-09-29T08:00:00.000Z" {
		t.Errorf("agent_ts = %v, want the UTC rendering", fields["agent_ts"])
	}
}

// Every constructor gives an object, so a consumer never has to handle a null
// detail.
func TestEventDetailIsNeverNull(t *testing.T) {
	builder := testBuilder()
	device := Device{ID: "scanner-1"}
	for _, event := range []Event{
		builder.PortOpened(device, "/dev/ttyACM0", at),
		builder.PortClosed(device, "/dev/ttyACM0", at),
		builder.PortLost(device, "/dev/ttyACM0", ErrorDisconnected, "read: input/output error", at),
		builder.OpenFailed(device, "/dev/ttyACM0", ErrorAbsent, "no such file or directory", at),
		builder.Discard(device, DiscardOversize, 300, at),
	} {
		if _, isObject := decode(t, event)["detail"].(map[string]any); !isObject {
			t.Errorf("%s: detail is not an object", event.Code)
		}
	}
}

// A tx that could not be read has no id or sender to copy; they are null, not
// empty strings a sender might match against.
func TestTxResultForUnreadableTx(t *testing.T) {
	result := testBuilder().TxResult(Device{ID: "printer-1"}, true, "", "", TxCodeInvalidMessage,
		TxOutcome{Reason: "invalid character 'x' looking for beginning of value"}, at)
	fields := decode(t, result)
	if fields["tx_id"] != nil || fields["sender"] != nil || fields["error_class"] != nil {
		t.Errorf("tx_id, sender, error_class = %v, %v, %v, want null", fields["tx_id"], fields["sender"], fields["error_class"])
	}
	if fields["state"] != "failed" {
		t.Errorf("state = %v, want failed", fields["state"])
	}
	want := "not a valid tx: invalid character 'x' looking for beginning of value"
	if fields["text"] != want {
		t.Errorf("text = %q, want %q", fields["text"], want)
	}
}

func TestTxResultUnknownCodePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("an unknown code was accepted, want a panic")
		}
	}()
	testBuilder().TxResult(Device{ID: "printer-1"}, true, "t", "s", TxCode("made_up"), TxOutcome{}, at)
}

// Every code has a state and a text of its own.
func TestEveryTxCodeHasStateAndText(t *testing.T) {
	codes := []TxCode{TxCodeAccepted, TxCodeWritten, TxCodeAlreadyWritten, TxCodeInvalidMessage,
		TxCodeInvalidID, TxCodePortUnavailable, TxCodeWriteFailed, TxCodeExpired}
	if len(codes) != len(txCodes) {
		t.Fatalf("%d codes listed here, %d in txCodes", len(codes), len(txCodes))
	}
	texts := map[string]TxCode{}
	for _, code := range codes {
		entry, known := txCodes[code]
		if !known || entry.state == "" || entry.text == "" {
			t.Errorf("%s: state %q, text %q", code, entry.state, entry.text)
		}
		if other, seen := texts[entry.text]; seen {
			t.Errorf("%s and %s share the text %q", code, other, entry.text)
		}
		texts[entry.text] = code
	}
}

// With no devices configured the keepalive still says so with an empty list.
func TestKeepaliveWithoutDevices(t *testing.T) {
	devices, isList := decode(t, testBuilder().Keepalive(at, at, 15*time.Second, nil))["devices"].([]any)
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
