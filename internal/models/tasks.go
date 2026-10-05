package models

import (
	"encoding/json"
	"strconv"

	"github.com/hookdeck/outpost/internal/mqs"
)

type EventTelemetry struct {
	TraceID      string
	SpanID       string
	ReceivedTime string // format time.RFC3339Nano
}

type DeliveryTelemetry struct {
	TraceID string
	SpanID  string
}

var _ mqs.IncomingMessage = &Event{}

func (e *Event) FromMessage(msg *mqs.Message) error {
	return json.Unmarshal(msg.Body, e)
}

func (e *Event) ToMessage() (*mqs.Message, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return &mqs.Message{Body: data}, nil
}

// DeliveryTask represents a task to deliver an event to a destination.
// This is a message type (no ID) used by: publishmq -> deliverymq, retry -> deliverymq
type DeliveryTask struct {
	Event         Event              `json:"event"`
	DestinationID string             `json:"destination_id"`
	Attempt       int                `json:"attempt"`
	Manual        bool               `json:"manual"`
	Telemetry     *DeliveryTelemetry `json:"telemetry,omitempty"`
	// Acceptance is set on the tasks a publish admitted by MB Wallet's deadline gate
	// enqueues. Such a task is delivered only once its call's acceptance record exists.
	Acceptance *Acceptance `json:"mb_acceptance,omitempty"`
}

// Acceptance ties a delivery task to the gated call that published it. Key names the
// record the call writes, through the Redis write fence, after every one of its tasks is
// enqueued; Fence is the call's fence in Redis server microseconds. Once Redis's clock
// has passed Fence the record can no longer be written, so a task whose record is still
// missing then was enqueued by a call that did not complete before its cut-off, and is
// never delivered.
type Acceptance struct {
	Key   string `json:"key"`
	Fence int64  `json:"fence"`
}

var _ mqs.IncomingMessage = &DeliveryTask{}

func (t *DeliveryTask) FromMessage(msg *mqs.Message) error {
	return json.Unmarshal(msg.Body, t)
}

func (t *DeliveryTask) ToMessage() (*mqs.Message, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return &mqs.Message{Body: data}, nil
}

// IdempotencyKey returns the key used for idempotency checks.
// Uses event_id:destination_id:attempt_number so that:
//   - Manual and auto retries with the same attempt_number are deduplicated (race protection)
//   - Each new attempt gets a fresh key (no need to clear on failure)
//   - MQ redeliveries of the same message are still deduplicated
func (t *DeliveryTask) IdempotencyKey() string {
	return t.Event.ID + ":" + t.DestinationID + ":" + strconv.Itoa(t.Attempt)
}

// RetryID returns the ID used for scheduling and canceling retries.
// Uses event_id:destination_id to allow manual retries to cancel pending automatic retries.
func RetryID(eventID, destinationID string) string {
	return eventID + ":" + destinationID
}

// NewDeliveryTask creates a new DeliveryTask for an event and destination.
func NewDeliveryTask(event Event, destinationID string) DeliveryTask {
	return DeliveryTask{
		Event:         event,
		DestinationID: destinationID,
		Attempt:       1,
	}
}

// NewManualDeliveryTask creates a new DeliveryTask for a manual retry.
// attemptNumber is the 1-indexed attempt number derived from the count of prior attempts.
func NewManualDeliveryTask(event Event, destinationID string, attemptNumber int) DeliveryTask {
	return DeliveryTask{
		Event:         event,
		DestinationID: destinationID,
		Attempt:       attemptNumber,
		Manual:        true,
	}
}

// LogEntry represents a message for the log queue.
//
// IMPORTANT: Both Event and Attempt are REQUIRED. The logstore requires both
// to exist for proper data consistency. The logmq consumer validates this
// requirement and rejects entries missing either field.
type LogEntry struct {
	Event       *Event       `json:"event"`
	Attempt     *Attempt     `json:"attempt"`
	Destination *Destination `json:"destination,omitempty"` // carried for alert evaluation in logmq; ignored by logstore
}

var _ mqs.IncomingMessage = &LogEntry{}

func (e *LogEntry) FromMessage(msg *mqs.Message) error {
	return json.Unmarshal(msg.Body, e)
}

func (e *LogEntry) ToMessage() (*mqs.Message, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return &mqs.Message{Body: data}, nil
}
