package wire

import (
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Schema is the version every message in this package carries. v1's messages
// are schema 1 (internal/event); consumers switch on it.
const Schema = 2

// TimeFormat is RFC 3339 with milliseconds. Timestamps are always rendered in
// UTC, because consumers compare them across stations in different zones.
const TimeFormat = "2006-01-02T15:04:05.000Z07:00"

// Kinds of message. A status topic carries two kinds, so every message says
// which one it is.
const (
	KindRx        = "rx"
	KindEvent     = "event"
	KindTxResult  = "tx_result"
	KindKeepalive = "keepalive"
	KindOffline   = "offline"
)

// Agent identifies the agent in every message it publishes.
type Agent struct {
	Project      string
	Site         string
	Station      string
	InstanceID   string
	AgentVersion string
}

// Device identifies one configured device in the messages about it.
type Device struct {
	ID string
	// Type is the configured device_type, carried and never interpreted. Empty
	// means none is configured, and is published as null.
	Type string
	// Expiry is the device's message expiry, reported in its status messages.
	Expiry time.Duration
}

// header opens every message the agent publishes. The identity repeats what
// the topic says, because a message copied into a log, a ticket or a database
// row loses its topic.
type header struct {
	Schema       int    `json:"schema"`
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	Project      string `json:"project"`
	Site         string `json:"site"`
	Station      string `json:"station"`
	InstanceID   string `json:"instance_id"`
	AgentVersion string `json:"agent_version"`
	// AgentTS is when the agent built the message, on the host clock, which
	// nothing here vouches for.
	AgentTS string `json:"agent_ts"`
}

// deviceHeader follows the header in every message about one device.
type deviceHeader struct {
	DeviceID   string  `json:"device_id"`
	DeviceType *string `json:"device_type"`
}

// Rx is one frame read from a device, published on the device's rx topic.
type Rx struct {
	header
	deviceHeader
	// Seq counts this device's frames from 1 at process start, so a gap between
	// two received values means frames were lost between them. It does not
	// survive a restart and is not a dedup key; the id is.
	Seq uint64 `json:"seq"`
	// RawB64 is the whole frame, separator excluded, in padded standard base64.
	// Nothing is stripped from it.
	RawB64 string `json:"raw_b64"`
	// Text is the frame when it is valid UTF-8, and null otherwise rather than
	// a lossy rendering.
	Text      *string `json:"text"`
	TextValid bool    `json:"text_valid"`
}

// EventCode names what happened to a device.
type EventCode string

// Event codes. The list is a minimum; codes are added, never renamed.
const (
	EventPortOpened EventCode = "port_opened"
	EventPortClosed EventCode = "port_closed"
	EventPortLost   EventCode = "port_lost"
	EventOpenFailed EventCode = "open_failed"
	EventDiscard    EventCode = "discard"
)

// ErrorClass names a port failure the way v1 classifies one
// (internal/device/serial/serial.go, classify).
type ErrorClass string

// Error classes.
const (
	ErrorAbsent           ErrorClass = "absent"
	ErrorBusy             ErrorClass = "busy"
	ErrorPermissionDenied ErrorClass = "permission_denied"
	ErrorReadOnly         ErrorClass = "read_only"
	ErrorDisconnected     ErrorClass = "disconnected"
	ErrorPortError        ErrorClass = "port_error"
	ErrorUnknown          ErrorClass = "unknown"
)

// DiscardReason says why the framer threw bytes away, in the framer's own
// names (internal/device/serial/framer.go).
type DiscardReason string

// Discard reasons.
const (
	DiscardOversize         DiscardReason = "oversize"
	DiscardInterCharTimeout DiscardReason = "inter_char_timeout"
	DiscardResync           DiscardReason = "resync"
	DiscardEmptyFrame       DiscardReason = "empty_frame"
)

// Event is something that happened to a device, published on the device's
// status topic.
type Event struct {
	header
	deviceHeader
	// DeviceOpen is whether the agent holds the port open after the event. It
	// is what the agent knows, not what the hardware is doing.
	DeviceOpen bool      `json:"device_open"`
	ExpiryS    int64     `json:"expiry_s"`
	Code       EventCode `json:"code"`
	// Text is a sentence for a person; nothing should parse it.
	Text string `json:"text"`
	// Detail holds the facts at hand, with keys that depend on the code. It is
	// an empty object, never null, for a code that has none.
	Detail map[string]any `json:"detail"`
}

// TxState is how far a tx got.
type TxState string

// Tx states.
const (
	TxAccepted TxState = "accepted"
	TxWritten  TxState = "written"
	TxFailed   TxState = "failed"
)

// TxCode says why a tx result has its state.
type TxCode string

// Tx result codes.
const (
	TxCodeAccepted        TxCode = "accepted"
	TxCodeWritten         TxCode = "written"
	TxCodeAlreadyWritten  TxCode = "already_written"
	TxCodeInvalidMessage  TxCode = "invalid_message"
	TxCodeInvalidID       TxCode = "invalid_id"
	TxCodePortUnavailable TxCode = "port_unavailable"
	TxCodeWriteFailed     TxCode = "write_failed"
	TxCodeExpired         TxCode = "expired"
)

// txCodes gives each code its state and the sentence its result opens with. A
// code missing here is a programming error, which TxResult reports by
// panicking.
var txCodes = map[TxCode]struct {
	state TxState
	text  string
}{
	TxCodeAccepted:        {TxAccepted, "received and queued for the port"},
	TxCodeWritten:         {TxWritten, "every byte reached the port"},
	TxCodeAlreadyWritten:  {TxWritten, "a tx with this id was written before, so it was not written again"},
	TxCodeInvalidMessage:  {TxFailed, "not a valid tx"},
	TxCodeInvalidID:       {TxFailed, "the id is not a UUID"},
	TxCodePortUnavailable: {TxFailed, "the port could not be opened"},
	TxCodeWriteFailed:     {TxFailed, "writing to the port failed after it had started"},
	TxCodeExpired:         {TxFailed, "the message expiry passed before the port could be written"},
}

// TxResult reports what happened to one tx, on the device's status topic.
type TxResult struct {
	header
	deviceHeader
	DeviceOpen bool  `json:"device_open"`
	ExpiryS    int64 `json:"expiry_s"`
	// TxID and Sender are copied from the tx, and null when it could not be
	// read.
	TxID   *string `json:"tx_id"`
	Sender *string `json:"sender"`
	State  TxState `json:"state"`
	Code   TxCode  `json:"code"`
	Text   string  `json:"text"`
	// ErrorClass is set when a port error caused the result, and null
	// otherwise.
	ErrorClass   *ErrorClass `json:"error_class"`
	BytesWritten int         `json:"bytes_written"`
	// Attempts counts the attempts made to open the port for this tx; it is 0
	// when the port was already open.
	Attempts int `json:"attempts"`
}

// TxOutcome is what the write path knows about one tx when it reports on it.
type TxOutcome struct {
	// ErrorClass is set when a port error caused the result.
	ErrorClass ErrorClass
	// Reason is added to the result's text: a parse error, the port's error.
	Reason       string
	BytesWritten int
	Attempts     int
}

// Tx is what a sender publishes on a device's tx topic. The agent reads it from
// 2.1.0 on; until then the topic is reserved.
type Tx struct {
	Schema int    `json:"schema"`
	ID     string `json:"id"`
	Sender string `json:"sender"`
	RawB64 string `json:"raw_b64"`
}

// DiscardCounts has one counter per discard reason. Every key is always
// present, so a consumer can difference two keepalives without handling a
// missing one.
type DiscardCounts struct {
	Oversize         uint64 `json:"oversize"`
	InterCharTimeout uint64 `json:"inter_char_timeout"`
	Resync           uint64 `json:"resync"`
	EmptyFrame       uint64 `json:"empty_frame"`
}

// OpenFailureCounts has one counter per error class, every key always present.
type OpenFailureCounts struct {
	Absent           uint64 `json:"absent"`
	Busy             uint64 `json:"busy"`
	PermissionDenied uint64 `json:"permission_denied"`
	ReadOnly         uint64 `json:"read_only"`
	Disconnected     uint64 `json:"disconnected"`
	PortError        uint64 `json:"port_error"`
	Unknown          uint64 `json:"unknown"`
}

// DeviceCounters are one device's counters since the process started.
type DeviceCounters struct {
	RxFrames        uint64            `json:"rx_frames"`
	RxBytes         uint64            `json:"rx_bytes"`
	Discards        DiscardCounts     `json:"discards"`
	FailedOpens     OpenFailureCounts `json:"failed_opens"`
	PublishFailures uint64            `json:"publish_failures"`
	TxWritten       uint64            `json:"tx_written"`
	TxFailed        uint64            `json:"tx_failed"`
	// BufferDepth is how many frames are waiting to be published now.
	BufferDepth int `json:"buffer_depth"`
}

// DeviceState is what the keepalive reports about one device.
type DeviceState struct {
	Device   Device
	Open     bool
	Counters DeviceCounters
}

// KeepaliveDevice is one device's entry in a keepalive.
type KeepaliveDevice struct {
	deviceHeader
	DeviceOpen bool  `json:"device_open"`
	ExpiryS    int64 `json:"expiry_s"`
	DeviceCounters
}

// Keepalive is the agent's periodic report, on the agent's status topic. It is
// how a consumer learns the agent is alive, and every device's state.
type Keepalive struct {
	header
	// UptimeS counts from process start, so a restart loop shows as a counter
	// that keeps returning to zero.
	UptimeS int64 `json:"uptime_s"`
	// IntervalS is the time until the next keepalive. A consumer that has seen
	// none for several intervals treats the agent as gone.
	IntervalS int64             `json:"interval_s"`
	Devices   []KeepaliveDevice `json:"devices"`
}

// OfflineReason says why an agent went offline.
type OfflineReason string

// Offline reasons.
const (
	// OfflineShutdown is published by the agent itself when it stops cleanly,
	// because a clean disconnect discards the will.
	OfflineShutdown OfflineReason = "shutdown"
	// OfflineWill is registered as the will, which the broker publishes when
	// the connection is lost without a disconnect.
	OfflineWill OfflineReason = "will"
)

// Offline says an agent stopped, on the agent's status topic.
type Offline struct {
	header
	Reason OfflineReason `json:"reason"`
}

// Builder produces the messages of one agent process.
type Builder struct {
	agent Agent
	newID func() string
}

// NewBuilder creates a builder. newID returns a fresh message id; nil means
// a random UUID, version 4. Tests pass their own to make output repeatable.
func NewBuilder(agent Agent, newID func() string) *Builder {
	if newID == nil {
		newID = uuid.NewString
	}
	return &Builder{agent: agent, newID: newID}
}

// Rx wraps one frame. seq is the device's frame counter.
func (builder *Builder) Rx(device Device, seq uint64, frame []byte, at time.Time) Rx {
	rx := Rx{
		header:       builder.header(KindRx, at),
		deviceHeader: deviceHeaderOf(device),
		Seq:          seq,
		RawB64:       base64.StdEncoding.EncodeToString(frame),
	}
	if utf8.Valid(frame) {
		text := string(frame)
		rx.Text, rx.TextValid = &text, true
	}
	return rx
}

// PortOpened reports that the agent opened the device's port.
func (builder *Builder) PortOpened(device Device, path string, at time.Time) Event {
	return builder.event(device, true, EventPortOpened, fmt.Sprintf("opened %s", path),
		map[string]any{"path": path}, at)
}

// PortClosed reports that the agent closed the device's port on purpose.
func (builder *Builder) PortClosed(device Device, path string, at time.Time) Event {
	return builder.event(device, false, EventPortClosed, fmt.Sprintf("closed %s", path),
		map[string]any{"path": path}, at)
}

// PortLost reports that an open port failed under the agent. errText is the
// operating system's message, which already names the path.
func (builder *Builder) PortLost(device Device, path string, class ErrorClass, errText string, at time.Time) Event {
	return builder.event(device, false, EventPortLost, fmt.Sprintf("port lost: %s (%s)", errText, class),
		map[string]any{"path": path, "error_class": class, "error": errText}, at)
}

// OpenFailed reports that opening the port failed, with the operating
// system's message.
func (builder *Builder) OpenFailed(device Device, path string, class ErrorClass, errText string, at time.Time) Event {
	return builder.event(device, false, EventOpenFailed, fmt.Sprintf("could not open the port: %s (%s)", errText, class),
		map[string]any{"path": path, "error_class": class, "error": errText}, at)
}

// Discard reports bytes the framer threw away. The port is open, since the
// bytes were read from it.
func (builder *Builder) Discard(device Device, reason DiscardReason, bytes int, at time.Time) Event {
	return builder.event(device, true, EventDiscard, fmt.Sprintf("discarded %d bytes: %s", bytes, reason),
		map[string]any{"reason": reason, "bytes": bytes}, at)
}

// TxResult reports on one tx. txID and sender are empty when the tx could not
// be read, and are then published as null. The state follows from the code.
func (builder *Builder) TxResult(device Device, deviceOpen bool, txID, sender string, code TxCode, outcome TxOutcome, at time.Time) TxResult {
	entry, known := txCodes[code]
	if !known {
		panic(fmt.Sprintf("wire: tx result code %q has no state", code))
	}
	text := entry.text
	if outcome.Reason != "" {
		text += ": " + outcome.Reason
	}
	result := TxResult{
		header:       builder.header(KindTxResult, at),
		deviceHeader: deviceHeaderOf(device),
		DeviceOpen:   deviceOpen,
		ExpiryS:      expirySeconds(device.Expiry),
		TxID:         nullable(txID),
		Sender:       nullable(sender),
		State:        entry.state,
		Code:         code,
		Text:         text,
		BytesWritten: outcome.BytesWritten,
		Attempts:     outcome.Attempts,
	}
	if outcome.ErrorClass != "" {
		class := outcome.ErrorClass
		result.ErrorClass = &class
	}
	return result
}

// Keepalive reports the agent and every device, in the order given.
func (builder *Builder) Keepalive(started, at time.Time, interval time.Duration, devices []DeviceState) Keepalive {
	entries := make([]KeepaliveDevice, 0, len(devices))
	for _, state := range devices {
		entries = append(entries, KeepaliveDevice{
			deviceHeader:   deviceHeaderOf(state.Device),
			DeviceOpen:     state.Open,
			ExpiryS:        expirySeconds(state.Device.Expiry),
			DeviceCounters: state.Counters,
		})
	}
	return Keepalive{
		header:    builder.header(KindKeepalive, at),
		UptimeS:   int64(at.Sub(started) / time.Second),
		IntervalS: int64(interval / time.Second),
		Devices:   entries,
	}
}

// Offline says the agent stopped. For the will, at is when the agent
// connected: the broker publishes a payload composed at connect time, so a
// will's agent_ts is not the time of death.
func (builder *Builder) Offline(reason OfflineReason, at time.Time) Offline {
	return Offline{header: builder.header(KindOffline, at), Reason: reason}
}

func (builder *Builder) event(device Device, open bool, code EventCode, text string, detail map[string]any, at time.Time) Event {
	return Event{
		header:       builder.header(KindEvent, at),
		deviceHeader: deviceHeaderOf(device),
		DeviceOpen:   open,
		ExpiryS:      expirySeconds(device.Expiry),
		Code:         code,
		Text:         text,
		Detail:       detail,
	}
}

func (builder *Builder) header(kind string, at time.Time) header {
	return header{
		Schema:       Schema,
		Kind:         kind,
		ID:           builder.newID(),
		Project:      builder.agent.Project,
		Site:         builder.agent.Site,
		Station:      builder.agent.Station,
		InstanceID:   builder.agent.InstanceID,
		AgentVersion: builder.agent.AgentVersion,
		AgentTS:      at.UTC().Format(TimeFormat),
	}
}

func deviceHeaderOf(device Device) deviceHeader {
	return deviceHeader{DeviceID: device.ID, DeviceType: nullable(device.Type)}
}

func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func expirySeconds(expiry time.Duration) int64 { return int64(expiry / time.Second) }
