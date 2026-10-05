package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Schema is the version every message in this package carries. v1's messages
// are schema 1 (internal/event); consumers switch on it.
const Schema = 2

// hyphenatedUUIDLength is the length of a UUID in its hyphenated form,
// 8-4-4-4-12 hex digits (RFC 9562, section 4).
const hyphenatedUUIDLength = 36

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
	EventPortOpened     EventCode = "port_opened"
	EventPortClosed     EventCode = "port_closed"
	EventPortLost       EventCode = "port_lost"
	EventPortOpenFailed EventCode = "port_open_failed"
	EventBytesDiscarded EventCode = "bytes_discarded"
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

// ErrorClasses is every error class, each with its counter in failed_opens.
var ErrorClasses = []ErrorClass{ErrorAbsent, ErrorBusy, ErrorPermissionDenied, ErrorReadOnly,
	ErrorDisconnected, ErrorPortError, ErrorUnknown}

// Known reports whether class is one of ErrorClasses.
func (class ErrorClass) Known() bool { return slices.Contains(ErrorClasses, class) }

// DiscardReason says why the framer threw bytes away
// (internal/device/serial/framer.go).
type DiscardReason string

// Discard reasons.
const (
	// DiscardOversize means max_frame_bytes was reached with no separator.
	DiscardOversize DiscardReason = "oversize"
	// DiscardInterCharTimeout means the inter-character timeout expired with
	// a partial frame buffered.
	DiscardInterCharTimeout DiscardReason = "inter_char_timeout"
	// DiscardResync means bytes were dropped while recovering to the next
	// separator after an earlier discard.
	DiscardResync DiscardReason = "resync"
	// DiscardEmptyFrame means two separators arrived back to back.
	DiscardEmptyFrame DiscardReason = "empty_frame"
)

// DiscardReasons is every discard reason, each with its counter in discards.
var DiscardReasons = []DiscardReason{DiscardOversize, DiscardInterCharTimeout, DiscardResync, DiscardEmptyFrame}

// Known reports whether reason is one of DiscardReasons.
func (reason DiscardReason) Known() bool { return slices.Contains(DiscardReasons, reason) }

// Event is something that happened to a device, published on the device's
// status topic.
type Event struct {
	header
	deviceHeader
	// DeviceOpen is whether the agent holds the port open after the event. It
	// is what the agent knows, not what the hardware is doing.
	DeviceOpen bool `json:"device_open"`
	// MessageExpiryS is the device's MQTT message expiry, in seconds.
	MessageExpiryS int64     `json:"message_expiry_s"`
	Code           EventCode `json:"code"`
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
	// TxRejected is for a tx the agent did not take on because a tx with the
	// same id is still being processed. The earlier one carries on.
	TxRejected TxState = "rejected"
)

// TxCode says why a tx result has its state.
type TxCode string

// Tx result codes.
const (
	TxCodeAccepted        TxCode = "accepted"
	TxCodeWritten         TxCode = "written"
	TxCodeAlreadyWritten  TxCode = "already_written"
	TxCodeInProgress      TxCode = "in_progress"
	TxCodeInvalidMessage  TxCode = "invalid_message"
	TxCodeInvalidID       TxCode = "invalid_id"
	TxCodePortUnavailable TxCode = "port_unavailable"
	TxCodeWriteFailed     TxCode = "write_failed"
	TxCodeExpired         TxCode = "expired"
	// TxCodeAgentStopping is a tx the agent stopped before writing, or part
	// way through (#19 Q1).
	TxCodeAgentStopping TxCode = "agent_stopping"
)

// txCodeStates is the state each code reports.
var txCodeStates = map[TxCode]TxState{
	TxCodeAccepted:        TxAccepted,
	TxCodeWritten:         TxWritten,
	TxCodeAlreadyWritten:  TxWritten,
	TxCodeInProgress:      TxRejected,
	TxCodeInvalidMessage:  TxFailed,
	TxCodeInvalidID:       TxFailed,
	TxCodePortUnavailable: TxFailed,
	TxCodeWriteFailed:     TxFailed,
	TxCodeExpired:         TxFailed,
	TxCodeAgentStopping:   TxFailed,
}

// TxStage is where a tx that is still being processed stands.
type TxStage string

// Tx stages.
const (
	TxQueued  TxStage = "queued"
	TxWriting TxStage = "writing"
)

// TxRef identifies the tx a result is about. Both fields are empty when the tx
// could not be read, and are then published as null.
type TxRef struct {
	ID     string
	Sender string
}

// TxResult reports what happened to one tx, on the device's status topic.
type TxResult struct {
	header
	deviceHeader
	DeviceOpen     bool  `json:"device_open"`
	MessageExpiryS int64 `json:"message_expiry_s"`
	// TxID and Sender are copied from the tx, and null when it could not be
	// read.
	TxID   *string `json:"tx_id"`
	Sender *string `json:"sender"`
	State  TxState `json:"state"`
	Code   TxCode  `json:"code"`
	// Text is a sentence for a person, with the cause when there is one.
	Text string `json:"text"`
	// Detail holds the facts at hand, with keys that depend on the code; an
	// empty object, never null, for a code that has none.
	Detail map[string]any `json:"detail"`
}

// Tx is what a sender publishes on a device's tx topic, or on a broadcast
// group's, for the agent to write to the device, or to each in the group.
type Tx struct {
	Schema int    `json:"schema"`
	ID     string `json:"id"`
	Sender string `json:"sender"`
	RawB64 string `json:"raw_b64"`
}

// TxProblem is why a tx cannot be taken: the code of its failed result, and
// the reason as text.
type TxProblem struct {
	Code TxCode
	Text string
}

// ReadTx reads a tx as a sender published it, and returns its id and sender
// and the bytes to write. All four fields are required and no other is
// accepted, so that a sender's mistake gets a failed result rather than being
// ignored (DESIGN-V2.md, "tx"). A tx that cannot be taken comes back with its
// problem, and with its id and sender wherever they could be read, so that the
// sender can tell which tx failed.
func ReadTx(payload []byte) (TxRef, []byte, *TxProblem) {
	ref := readTxRef(payload)
	invalid := func(format string, args ...any) (TxRef, []byte, *TxProblem) {
		return ref, nil, &TxProblem{Code: TxCodeInvalidMessage, Text: fmt.Sprintf(format, args...)}
	}
	var fields struct {
		Schema *int    `json:"schema"`
		ID     *string `json:"id"`
		Sender *string `json:"sender"`
		RawB64 *string `json:"raw_b64"`
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fields); err != nil {
		return invalid("not a JSON object of schema, id, sender and raw_b64: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid("more than one JSON value")
	}
	var missing []string
	for _, field := range []struct {
		name    string
		missing bool
	}{{"schema", fields.Schema == nil}, {"id", fields.ID == nil}, {"sender", fields.Sender == nil}, {"raw_b64", fields.RawB64 == nil}} {
		if field.missing {
			missing = append(missing, field.name)
		}
	}
	if len(missing) > 0 {
		return invalid("missing %v", missing)
	}
	if *fields.Schema != Schema {
		return invalid("schema is %d; this agent reads schema %d", *fields.Schema, Schema)
	}
	if *fields.Sender == "" {
		return invalid("sender is empty")
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(*fields.RawB64)
	if err != nil {
		return invalid("raw_b64 is not padded standard base64: %v", err)
	}
	if len(raw) == 0 {
		return invalid("raw_b64 holds no bytes")
	}
	// The hyphenated form only: uuid.Parse also takes braces, a urn: prefix
	// and bare hex, none of which a sender means as an id.
	if _, err := uuid.Parse(*fields.ID); err != nil || len(*fields.ID) != hyphenatedUUIDLength {
		return ref, nil, &TxProblem{Code: TxCodeInvalidID, Text: fmt.Sprintf("the id %q is not a UUID", *fields.ID)}
	}
	return ref, raw, nil
}

// readTxRef takes the id and sender from a tx wherever they are strings, so
// that even a tx that fails names itself in its result.
func readTxRef(payload []byte) TxRef {
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return TxRef{}
	}
	var ref TxRef
	_ = json.Unmarshal(object["id"], &ref.ID)
	_ = json.Unmarshal(object["sender"], &ref.Sender)
	return ref
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

// Add counts one discard. A reason that is not one of DiscardReasons has no
// counter and is not counted.
func (counts *DiscardCounts) Add(reason DiscardReason) {
	switch reason {
	case DiscardOversize:
		counts.Oversize++
	case DiscardInterCharTimeout:
		counts.InterCharTimeout++
	case DiscardResync:
		counts.Resync++
	case DiscardEmptyFrame:
		counts.EmptyFrame++
	}
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

// Add counts one failed open. A class that is not one of ErrorClasses is
// counted as unknown.
func (counts *OpenFailureCounts) Add(class ErrorClass) {
	switch class {
	case ErrorAbsent:
		counts.Absent++
	case ErrorBusy:
		counts.Busy++
	case ErrorPermissionDenied:
		counts.PermissionDenied++
	case ErrorReadOnly:
		counts.ReadOnly++
	case ErrorDisconnected:
		counts.Disconnected++
	case ErrorPortError:
		counts.PortError++
	default:
		counts.Unknown++
	}
}

// DeviceCounters are one device's counters since the process started, as
// DESIGN-V2.md, "Agent keepalive", defines them.
type DeviceCounters struct {
	// RxFrames counts frames taken for publishing: the last rx seq.
	RxFrames uint64 `json:"rx_frames"`
	// RxBytes counts every byte read from the port, framed or not, so that it
	// rises while RxFrames stays flat when nothing is framed.
	RxBytes     uint64            `json:"rx_bytes"`
	Discards    DiscardCounts     `json:"discards"`
	FailedOpens OpenFailureCounts `json:"failed_opens"`
	// PublishFailures counts readings the broker did not take.
	PublishFailures uint64 `json:"publish_failures"`
	TxWritten       uint64 `json:"tx_written"`
	TxFailed        uint64 `json:"tx_failed"`
	// BufferDepth is how many frames are waiting to be published now.
	BufferDepth int `json:"buffer_depth"`
}

// DeviceState is what the keepalive reports about one device.
type DeviceState struct {
	Device   Device
	Open     bool
	Counters DeviceCounters
	// TxRoutes are the topics that reach the device's tx, its own first, each
	// with the broker's answer to its subscription.
	TxRoutes []TxRouteState
}

// TxRouteState is a topic that reaches a device's tx, as the agent subscribed
// to it, with the broker's answer on the current connection.
type TxRouteState struct {
	Route  TxRoute
	Answer SubscribeAnswer
}

// SubscribeAnswer is the broker's SUBACK reason code for one topic filter.
// Answered is false until the broker has answered on the current connection.
type SubscribeAnswer struct {
	Code     byte
	Answered bool
}

// TxTopic is one topic in a keepalive that reaches the device's tx: the topic
// filter as it went into the SUBSCRIBE packet, what it reaches, and the
// broker's answer (DESIGN-V2.md, "Broadcast groups").
type TxTopic struct {
	Topic string  `json:"topic"`
	Scope TxScope `json:"scope"`
	// Group is the broadcast group's name, null for the device's own topic.
	Group *string `json:"group"`
	// Suback is the broker's SUBACK reason code for the topic on the current
	// connection: 0 to 2 grant that QoS, 128 and above refuse it. Null until
	// the broker has answered.
	Suback *int `json:"suback"`
}

// KeepaliveDevice is one device's entry in a keepalive.
type KeepaliveDevice struct {
	deviceHeader
	DeviceOpen     bool      `json:"device_open"`
	MessageExpiryS int64     `json:"message_expiry_s"`
	TxTopics       []TxTopic `json:"tx_topics"`
	DeviceCounters
}

// Keepalive is the agent's periodic report, on the agent's status topic. It is
// how a consumer learns the agent is alive, and every device's state.
type Keepalive struct {
	header
	// UptimeS counts from process start, so a restart loop shows as a counter
	// that keeps returning to zero.
	UptimeS int64 `json:"uptime_s"`
	// IntervalS is the time until the next keepalive.
	IntervalS int64 `json:"interval_s"`
	// GoneAfterS is how long a consumer waits without a keepalive before it
	// treats the agent as gone: the configured number of missed intervals.
	GoneAfterS int64             `json:"gone_after_s"`
	Devices    []KeepaliveDevice `json:"devices"`
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

// PortOpenFailed reports one failed attempt to open the port, with the
// operating system's message. It is published on every attempt.
func (builder *Builder) PortOpenFailed(device Device, path string, class ErrorClass, errText string, at time.Time) Event {
	return builder.event(device, false, EventPortOpenFailed, fmt.Sprintf("could not open the port: %s (%s)", errText, class),
		map[string]any{"path": path, "error_class": class, "error": errText}, at)
}

// BytesDiscarded reports bytes the framer threw away. The port is open, since
// the bytes were read from it.
func (builder *Builder) BytesDiscarded(device Device, reason DiscardReason, bytes int, at time.Time) Event {
	return builder.event(device, true, EventBytesDiscarded, fmt.Sprintf("discarded %d bytes: %s", bytes, reason),
		map[string]any{"reason": reason, "bytes": bytes}, at)
}

// TxAccepted reports that a tx was received and queued for the port.
func (builder *Builder) TxAccepted(device Device, deviceOpen bool, tx TxRef, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeAccepted, "received and queued for the port",
		map[string]any{}, at)
}

// TxWritten reports that every byte of a tx reached the port. openAttempts is
// how many times the port had to be opened for it, 0 when it was open.
func (builder *Builder) TxWritten(device Device, deviceOpen bool, tx TxRef, bytesWritten, openAttempts int, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeWritten, fmt.Sprintf("all %d bytes reached the port", bytesWritten),
		map[string]any{"bytes_written": bytesWritten, "open_attempts": openAttempts}, at)
}

// TxAlreadyWritten reports that a tx with this id was written before, so this
// one was not.
func (builder *Builder) TxAlreadyWritten(device Device, deviceOpen bool, tx TxRef, writtenAt, at time.Time) TxResult {
	written := writtenAt.UTC().Format(TimeFormat)
	return builder.txResult(device, deviceOpen, tx, TxCodeAlreadyWritten,
		fmt.Sprintf("a tx with this id was written at %s, so this one was not written", written),
		map[string]any{"written_at": written}, at)
}

// TxInProgress rejects a tx whose id belongs to a tx still queued or being
// written, and says where that one stands and since when.
func (builder *Builder) TxInProgress(device Device, deviceOpen bool, tx TxRef, stage TxStage, since time.Time, bytesWritten int, at time.Time) TxResult {
	sinceText := since.UTC().Format(TimeFormat)
	text := fmt.Sprintf("a tx with this id is queued for the port since %s; this one was not taken", sinceText)
	if stage == TxWriting {
		text = fmt.Sprintf("a tx with this id is being written since %s, %d bytes so far; this one was not taken", sinceText, bytesWritten)
	}
	return builder.txResult(device, deviceOpen, tx, TxCodeInProgress, text,
		map[string]any{"stage": stage, "since": sinceText, "bytes_written": bytesWritten}, at)
}

// TxInvalidMessage reports a tx that could not be read. errText says why.
func (builder *Builder) TxInvalidMessage(device Device, deviceOpen bool, tx TxRef, errText string, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeInvalidMessage, "not a valid tx: "+errText,
		map[string]any{"error": errText}, at)
}

// TxInvalidID reports a tx whose id is not a UUID.
func (builder *Builder) TxInvalidID(device Device, deviceOpen bool, tx TxRef, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeInvalidID, "the id is not a UUID",
		map[string]any{}, at)
}

// TxPortUnavailable reports a tx given up because the port could not be
// opened within the configured attempts.
func (builder *Builder) TxPortUnavailable(device Device, deviceOpen bool, tx TxRef, class ErrorClass, errText string, openAttempts int, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodePortUnavailable,
		fmt.Sprintf("the port could not be opened in %d attempts: %s (%s)", openAttempts, errText, class),
		map[string]any{"error_class": class, "error": errText, "open_attempts": openAttempts}, at)
}

// TxWriteFailed reports a write that failed after it had started. It is not
// retried; bytesWritten says how far it got.
func (builder *Builder) TxWriteFailed(device Device, deviceOpen bool, tx TxRef, class ErrorClass, errText string, bytesWritten, openAttempts int, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeWriteFailed,
		fmt.Sprintf("writing failed after %d bytes: %s (%s)", bytesWritten, errText, class),
		map[string]any{"error_class": class, "error": errText, "bytes_written": bytesWritten, "open_attempts": openAttempts}, at)
}

// TxExpired reports a tx whose message expiry passed before an attempt to
// write it could start.
func (builder *Builder) TxExpired(device Device, deviceOpen bool, tx TxRef, openAttempts int, at time.Time) TxResult {
	return builder.txResult(device, deviceOpen, tx, TxCodeExpired, "the message expiry passed before the port could be written",
		map[string]any{"open_attempts": openAttempts}, at)
}

// TxAgentStopping reports a tx the agent stopped before writing, with
// bytesWritten 0, or part way through, with how far it got.
func (builder *Builder) TxAgentStopping(device Device, deviceOpen bool, tx TxRef, bytesWritten int, at time.Time) TxResult {
	text := "the agent stopped before this tx was written"
	if bytesWritten > 0 {
		text = fmt.Sprintf("the agent stopped while this tx was being written, after %d bytes", bytesWritten)
	}
	return builder.txResult(device, deviceOpen, tx, TxCodeAgentStopping, text,
		map[string]any{"bytes_written": bytesWritten}, at)
}

func (builder *Builder) txResult(device Device, deviceOpen bool, tx TxRef, code TxCode, text string, detail map[string]any, at time.Time) TxResult {
	return TxResult{
		header:         builder.header(KindTxResult, at),
		deviceHeader:   deviceHeaderOf(device),
		DeviceOpen:     deviceOpen,
		MessageExpiryS: messageExpirySeconds(device.Expiry),
		TxID:           nilIfEmpty(tx.ID),
		Sender:         nilIfEmpty(tx.Sender),
		State:          txCodeStates[code],
		Code:           code,
		Text:           text,
		Detail:         detail,
	}
}

// Keepalive reports the agent and every device, in the order given. missed is
// how many intervals a consumer waits for before it treats the agent as gone.
func (builder *Builder) Keepalive(started, at time.Time, interval time.Duration, missed int, devices []DeviceState) Keepalive {
	entries := make([]KeepaliveDevice, 0, len(devices))
	for _, state := range devices {
		txTopics := make([]TxTopic, 0, len(state.TxRoutes))
		for _, routeState := range state.TxRoutes {
			txTopic := TxTopic{Topic: routeState.Route.Topic, Scope: routeState.Route.Scope, Group: nilIfEmpty(routeState.Route.Group)}
			if routeState.Answer.Answered {
				code := int(routeState.Answer.Code)
				txTopic.Suback = &code
			}
			txTopics = append(txTopics, txTopic)
		}
		entries = append(entries, KeepaliveDevice{
			deviceHeader:   deviceHeaderOf(state.Device),
			DeviceOpen:     state.Open,
			MessageExpiryS: messageExpirySeconds(state.Device.Expiry),
			TxTopics:       txTopics,
			DeviceCounters: state.Counters,
		})
	}
	return Keepalive{
		header:     builder.header(KindKeepalive, at),
		UptimeS:    int64(at.Sub(started) / time.Second),
		IntervalS:  int64(interval / time.Second),
		GoneAfterS: int64(interval/time.Second) * int64(missed),
		Devices:    entries,
	}
}

// Offline says the agent stopped, or, as the will, that its connection was
// lost. For the will, at is when the connection it is composed for was made:
// the broker publishes the payload it was given at connect time, so a will's
// agent_ts is not the time of death.
func (builder *Builder) Offline(reason OfflineReason, at time.Time) Offline {
	return Offline{header: builder.header(KindOffline, at), Reason: reason}
}

func (builder *Builder) event(device Device, open bool, code EventCode, text string, detail map[string]any, at time.Time) Event {
	return Event{
		header:         builder.header(KindEvent, at),
		deviceHeader:   deviceHeaderOf(device),
		DeviceOpen:     open,
		MessageExpiryS: messageExpirySeconds(device.Expiry),
		Code:           code,
		Text:           text,
		Detail:         detail,
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
	return deviceHeader{DeviceID: device.ID, DeviceType: nilIfEmpty(device.Type)}
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// messageExpirySeconds is a device's message expiry as message_expiry_s
// carries it. The configuration holds every message expiry to whole seconds,
// so nothing is lost.
func messageExpirySeconds(expiry time.Duration) int64 { return int64(expiry / time.Second) }
