package core

import "sync"

// waitingQueue holds items in the order they were pushed, for one consumer
// that waits for them. Any goroutine may push; close says that none will
// again.
type waitingQueue[Item any] struct {
	mu     sync.Mutex
	items  []Item
	closed bool
	// ready holds a value after every push and at close, so that the
	// consumer need not poll.
	ready chan struct{}
}

func newWaitingQueue[Item any]() *waitingQueue[Item] {
	return &waitingQueue[Item]{ready: make(chan struct{}, 1)}
}

// push adds item at the end.
func (queue *waitingQueue[Item]) push(item Item) {
	queue.pushEvicting(item, nil)
}

// pushEvicting adds item at the end. First, under the same lock, evict is
// given the items held and returns the index of one to remove, or -1; the
// removed item is returned, with true. A nil evict removes none.
func (queue *waitingQueue[Item]) pushEvicting(item Item, evict func(held []Item) int) (Item, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	var evicted Item
	removed := false
	if evict != nil {
		if index := evict(queue.items); index >= 0 {
			evicted, removed = queue.items[index], true
			queue.items = append(queue.items[:index], queue.items[index+1:]...)
		}
	}
	queue.items = append(queue.items, item)
	queue.signal()
	return evicted, removed
}

// close says that nothing will be pushed again.
func (queue *waitingQueue[Item]) close() {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.closed = true
	queue.signal()
}

// wait blocks until the queue holds an item, and returns false instead once
// it is closed and empty.
func (queue *waitingQueue[Item]) wait() bool {
	for {
		queue.mu.Lock()
		held, closed := len(queue.items), queue.closed
		queue.mu.Unlock()
		switch {
		case held > 0:
			return true
		case closed:
			return false
		}
		<-queue.ready
	}
}

// pop takes the oldest item, if there is one, without waiting.
func (queue *waitingQueue[Item]) pop() (Item, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	var oldest Item
	if len(queue.items) == 0 {
		return oldest, false
	}
	oldest = queue.items[0]
	queue.items = queue.items[1:]
	return oldest, true
}

// next waits for an item and takes it, and returns false once the queue is
// closed and empty.
func (queue *waitingQueue[Item]) next() (Item, bool) {
	for queue.wait() {
		if item, ok := queue.pop(); ok {
			return item, true
		}
	}
	var none Item
	return none, false
}

// signal wakes the consumer. It is called with mu held.
func (queue *waitingQueue[Item]) signal() {
	select {
	case queue.ready <- struct{}{}:
	default:
	}
}

// statusQueue holds one device's status messages, port events and tx
// results, until they can be published, oldest first. It keeps at most
// eventLimit events: pushing an event when that many are held drops the
// oldest, so that after a long broker outage the most recent events are the
// ones left (#13 Q1). A tx result is never dropped for room. Its sender waits
// for it and resends without it, and a printer prints a resent job twice.
// Results cannot pile up during an outage either, since no tx arrives without
// the connection.
type statusQueue struct {
	*waitingQueue[statusItem]
	eventLimit int
}

func newStatusQueue(eventLimit int) *statusQueue {
	return &statusQueue{waitingQueue: newWaitingQueue[statusItem](), eventLimit: eventLimit}
}

// push adds a message. When an event found eventLimit events held, it returns
// the oldest, which it dropped to make room, and true.
func (queue *statusQueue) push(item statusItem) (statusItem, bool) {
	if item.event == nil {
		queue.waitingQueue.push(item)
		return statusItem{}, false
	}
	return queue.pushEvicting(item, func(held []statusItem) int {
		events, oldest := 0, -1
		for index, queued := range held {
			if queued.event != nil {
				events++
				if oldest < 0 {
					oldest = index
				}
			}
		}
		if events < queue.eventLimit {
			return -1
		}
		return oldest
	})
}
