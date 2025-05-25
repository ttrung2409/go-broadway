package broadway

import (
	"sync"
)

// concurrentSlice is a generic thread-safe slice implementation.
// It provides basic slice operations with mutex synchronization
// to allow safe concurrent access from multiple goroutines.
//
// Type parameter T can be any type.
type concurrentSlice[T comparable] struct {
	items []T
	mu    sync.Mutex
}

// newConcurrentSlice creates a new concurrent slice.
func newConcurrentSlice[T comparable]() *concurrentSlice[T] {
	return &concurrentSlice[T]{items: make([]T, 0), mu: sync.Mutex{}}
}

// Add appends one or more items to the end of the slice.
//
// Parameters:
//   - items: The items to be added to the end of the slice
func (s *concurrentSlice[T]) Add(items ...T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, items...)
}

// Shift removes and returns the first item in the slice.
//
// Returns:
//   - The first item in the slice
//   - true if an item was successfully removed, false otherwise
func (s *concurrentSlice[T]) Shift() (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		var zeroValue T
		return zeroValue, false
	}

	item := s.items[0]
	s.items = s.items[1:]
	return item, true
}

// Each applies a function to each item in the slice without removing the items.
// The slice remains unchanged after this operation.
//
// Parameters:
//   - do: A function to apply to each item in the slice
func (s *concurrentSlice[T]) Each(do func(item T)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range s.items {
		do(item)
	}
}

// Filter removes items from the slice that don't satisfy the predicate function.
// It returns a new slice containing only the items that satisfy the predicate.
//
// Parameters:
//   - predicate: A function that returns true for items to keep and false for items to remove
//
// Returns:
//   - The slice itself after filtering, allowing for method chaining
func (s *concurrentSlice[T]) Filter(predicate func(item T) bool) *concurrentSlice[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	filtered := make([]T, 0)

	for _, item := range s.items {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}

	s.items = filtered

	return s
}

// Len returns the number of items currently in the slice.
//
// Returns:
//   - The number of items in the slice
func (s *concurrentSlice[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.items)
}

// IsEmpty checks if the slice is empty.
//
// Returns:
//   - true if the slice contains no items, false otherwise
func (s *concurrentSlice[T]) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.items) == 0
}

// Drain removes all items from the slice and returns them.
//
// Returns:
//   - A new slice containing all the items
func (s *concurrentSlice[T]) Drain() []T {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := s.items
	s.items = make([]T, 0)
	return items
}

func (s *concurrentSlice[T]) Find(predicate func(item T) bool) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		var zeroValue T
		return zeroValue, false
	}

	for _, item := range s.items {
		if predicate(item) {
			return item, true
		}
	}

	var zeroValue T
	return zeroValue, false
}

func (s *concurrentSlice[T]) Prepend(items ...T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, items...)
}

func (s *concurrentSlice[T]) Remove(item T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, v := range s.items {
		if v == item {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return true
		}
	}

	return false
}
