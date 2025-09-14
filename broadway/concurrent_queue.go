package broadway

import (
	"sync"
)

// concurrentQueue is a generic thread-safe queue implementation.
// It provides basic queue operations with mutex synchronization
// to allow safe concurrent access from multiple goroutines.
//
// Type parameter T can be any type.
type concurrentQueue[T any] struct {
	_queue []T
	_mu    sync.RWMutex
}

func newConcurrentQueue[T any]() *concurrentQueue[T] {
	return &concurrentQueue[T]{_queue: make([]T, 0), _mu: sync.RWMutex{}}
}

// enqueue adds one or more items to the end of the queue.
//
// Parameters:
//   - items: The items to be added to the end of the queue
func (q *concurrentQueue[T]) enqueue(items ...T) {
	q._mu.Lock()
	defer q._mu.Unlock()
	q._queue = append(q._queue, items...)
}

// prepend adds one or more items to the beginning of the queue.
//
// Parameters:
//   - items: The items to be added to the beginning of the queue
func (q *concurrentQueue[T]) prepend(items ...T) {
	q._mu.Lock()
	defer q._mu.Unlock()
	q._queue = append(items, q._queue...)
}

// dequeue removes and returns up to 'total' items from the queue.
// If the queue contains fewer than 'total' items, all available items are returned.
//
// Parameters:
//   - total: The maximum number of items to dequeue
//
// Returns:
//   - A slice containing the dequeued items (may be fewer than requested)
//   - true if at least one item was dequeued, false otherwise
func (q *concurrentQueue[T]) dequeue(total int) ([]T, bool) {
	q._mu.Lock()
	defer q._mu.Unlock()

	if len(q._queue) < total {
		items := q._queue
		q._queue = make([]T, 0)
		return items, len(items) > 0
	}

	items := q._queue[:total]
	q._queue = q._queue[total:]
	return items, len(items) > 0
}

// dequeueAll removes and returns all items currently in the queue.
//
// Returns:
//   - A slice containing all items that were in the queue
//   - true if at least one item was in the queue, false otherwise
func (q *concurrentQueue[T]) dequeueAll() ([]T, bool) {
	q._mu.Lock()
	defer q._mu.Unlock()

	items := q._queue
	q._queue = make([]T, 0)
	return items, len(items) > 0
}

// len returns the number of items currently in the queue.
//
// Returns:
//   - The number of items in the queue
func (q *concurrentQueue[T]) len() int {
	q._mu.RLock()
	defer q._mu.RUnlock()

	return len(q._queue)
}

func (q *concurrentQueue[T]) toSlice() []T {
	q._mu.RLock()
	defer q._mu.RUnlock()

	snapshot := make([]T, len(q._queue))
	copy(snapshot, q._queue)
	return snapshot
}
