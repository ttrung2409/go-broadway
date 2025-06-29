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
	queue []T
	mu    sync.RWMutex
}

func newConcurrentQueue[T any]() *concurrentQueue[T] {
	return &concurrentQueue[T]{queue: make([]T, 0), mu: sync.RWMutex{}}
}

// enqueue adds one or more items to the end of the queue.
//
// Parameters:
//   - items: The items to be added to the end of the queue
func (q *concurrentQueue[T]) enqueue(items ...T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, items...)
}

// prepend adds one or more items to the beginning of the queue.
//
// Parameters:
//   - items: The items to be added to the beginning of the queue
func (q *concurrentQueue[T]) prepend(items ...T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(items, q.queue...)
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
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) < total {
		items := q.queue
		q.queue = make([]T, 0)
		return items, len(items) > 0
	}

	items := q.queue[:total]
	q.queue = q.queue[total:]
	return items, len(items) > 0
}

// dequeueAll removes and returns all items currently in the queue.
//
// Returns:
//   - A slice containing all items that were in the queue
//   - true if at least one item was in the queue, false otherwise
func (q *concurrentQueue[T]) dequeueAll() ([]T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := q.queue
	q.queue = make([]T, 0)
	return items, len(items) > 0
}

// len returns the number of items currently in the queue.
//
// Returns:
//   - The number of items in the queue
func (q *concurrentQueue[T]) len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.queue)
}

func (q *concurrentQueue[T]) toSlice() []T {
	q.mu.RLock()
	defer q.mu.RUnlock()

	snapshot := make([]T, len(q.queue))
	copy(snapshot, q.queue)
	return snapshot
}
