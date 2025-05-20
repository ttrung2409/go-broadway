package broadway

import (
	"sync"
)

type concurrentQueue[T any] struct {
	queue []T
	mu    sync.Mutex
}

func newConcurrentQueue[T any]() *concurrentQueue[T] {
	return &concurrentQueue[T]{queue: make([]T, 0), mu: sync.Mutex{}}
}

func (q *concurrentQueue[T]) Enqueue(items ...T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, items...)
}

func (q *concurrentQueue[T]) Prepend(items ...T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(items, q.queue...)
}

func (q *concurrentQueue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		var zeroValue T
		return zeroValue, false
	}

	item := q.queue[0]
	q.queue = q.queue[1:]
	return item, true
}

func (q *concurrentQueue[T]) DequeueMany(count int) ([]T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) < count {
		return q.queue[:len(q.queue)], len(q.queue) > 0
	}

	items := q.queue[:count]
	q.queue = q.queue[count:]
	return items, len(items) > 0
}

func (q *concurrentQueue[T]) DequeueAll() ([]T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.queue, len(q.queue) > 0
}

func (q *concurrentQueue[T]) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue) == 0
}

func (q *concurrentQueue[T]) Each(do func(item T)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range q.queue {
		do(item)
	}
}

func (q *concurrentQueue[T]) Filter(predicate func(item T) bool) *concurrentQueue[T] {
	q.mu.Lock()
	defer q.mu.Unlock()

	filtered := make([]T, 0)

	for _, item := range q.queue {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}

	q.queue = filtered

	return q
}

func (q *concurrentQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.queue)
}
