package broadway

import (
	"sync"
)

// concurrentSlice is a generic thread-safe slice implementation.
// It provides basic slice operations with RWMutex synchronization
// to allow safe concurrent access from multiple goroutines.
// Read operations can occur concurrently, while write operations
// have exclusive access.
//
// Type parameter T can be any type.
type concurrentSlice[T comparable] struct {
	items []T
	mu    sync.RWMutex
}

// newConcurrentSlice creates a new concurrent slice.
func newConcurrentSlice[T comparable]() *concurrentSlice[T] {
	return &concurrentSlice[T]{items: make([]T, 0), mu: sync.RWMutex{}}
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

// Filter removes items from the slice that don't satisfy the predicate function.
// It returns a new slice containing only the items that satisfy the predicate.
//
// Parameters:
//   - predicate: A function that returns true for items to keep and false for items to remove
//
// Returns:
//   - A new concurrent slice after filtering
func (s *concurrentSlice[T]) Filter(predicate func(item T) bool) *concurrentSlice[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filtered := make([]T, 0)

	for _, item := range s.items {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}

	return &concurrentSlice[T]{
		items: filtered,
		mu:    sync.RWMutex{},
	}
}

// Len returns the number of items currently in the slice.
//
// Returns:
//   - The number of items in the slice
func (s *concurrentSlice[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}

// IsEmpty checks if the slice is empty.
//
// Returns:
//   - true if the slice contains no items, false otherwise
func (s *concurrentSlice[T]) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

// Find searches for an item in the slice that satisfies the given predicate.
//
// Parameters:
//   - predicate: A function that returns true when the desired item is found
//
// Returns:
//   - The first item that satisfies the predicate
//   - true if such an item was found, false otherwise
func (s *concurrentSlice[T]) Find(predicate func(item T) bool) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

// Prepend adds one or more items to the beginning of the slice.
//
// Parameters:
//   - items: The items to be added to the beginning of the slice
func (s *concurrentSlice[T]) Prepend(items ...T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(items, s.items...)
}

// Remove removes the first occurrence of the specified item from the slice.
//
// Parameters:
//   - item: The item to remove from the slice
//
// Returns:
//   - true if the item was found and removed, false otherwise
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

func (s *concurrentSlice[T]) ToSlice() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]T, len(s.items))
	copy(items, s.items)

	return items
}
