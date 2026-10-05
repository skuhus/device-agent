package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/wire"
)

// Items come out in the order they went in, and closing an empty queue ends
// the consumer's wait.
func TestWaitingQueueKeepsOrderAndEndsAtClose(t *testing.T) {
	queue := newWaitingQueue[int]()
	for item := 1; item <= 3; item++ {
		queue.push(item)
	}
	for want := 1; want <= 3; want++ {
		if got, ok := queue.next(); !ok || got != want {
			t.Fatalf("next = %d, %t; want %d", got, ok, want)
		}
	}

	ended := make(chan bool, 1)
	go func() {
		_, ok := queue.next()
		ended <- ok
	}()
	queue.close()
	select {
	case ok := <-ended:
		if ok {
			t.Error("next on a closed, empty queue returned an item")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the queue did not end the consumer's wait")
	}
}

// What was pushed before close is still taken after it.
func TestWaitingQueueDrainsAfterClose(t *testing.T) {
	queue := newWaitingQueue[string]()
	queue.push("last")
	queue.close()
	if got, ok := queue.next(); !ok || got != "last" {
		t.Errorf("next = %q, %t; want the item pushed before close", got, ok)
	}
	if _, ok := queue.next(); ok {
		t.Error("next returned an item after the queue was drained")
	}
}

// A full queue drops its oldest event to make room, and never a tx result:
// a sender that hears nothing resends, and a printer prints the job twice.
func TestStatusQueueDropsTheOldestEventNeverATxResult(t *testing.T) {
	queue := newStatusQueue(2)
	event := func(bytes int) statusItem {
		return statusItem{event: &device.Event{Kind: device.BytesDiscarded, Reason: wire.DiscardOversize, Bytes: bytes}}
	}
	result := statusItem{tx: &txResultItem{result: wire.TxResult{State: wire.TxWritten}}}

	queue.push(result)
	queue.push(event(1))
	queue.push(event(2))
	dropped, full := queue.push(event(3))
	if !full || dropped.event == nil || dropped.event.Bytes != 1 {
		t.Fatalf("pushing a third event dropped %+v (full %t), want the first event", dropped, full)
	}

	var kept []string
	for {
		item, ok := queue.pop()
		if !ok {
			break
		}
		if item.tx != nil {
			kept = append(kept, "result")
		} else {
			kept = append(kept, fmt.Sprint(item.event.Bytes))
		}
	}
	if got := fmt.Sprint(kept); got != "[result 2 3]" {
		t.Errorf("the queue held %s, want the tx result and the two most recent events", got)
	}
}
