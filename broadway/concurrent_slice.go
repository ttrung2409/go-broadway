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
	_slice []T
	_mu    sync.RWMutex
}

// newConcurrentSlice creates a new concurrent slice.
func newConcurrentSlice[T comparable]() *concurrentSlice[T] {
	return &concurrentSlice[T]{_slice: make([]T, 0), _mu: sync.RWMutex{}}
}

// add appends one or more items to the end of the slice.
//
// Parameters:
//   - items: The items to be added to the end of the slice
func (s *concurrentSlice[T]) add(items ...T) {
	s._mu.Lock()
	defer s._mu.Unlock()
	s._slice = append(s._slice, items...)
}

// filter removes items from the slice that don't satisfy the predicate function.
// It returns a new slice containing only the items that satisfy the predicate.
//
// Parameters:
//   - predicate: A function that returns true for items to keep and false for items to remove
//
// Returns:
//   - A new concurrent slice after filtering
func (s *concurrentSlice[T]) filter(predicate func(item T) bool) *concurrentSlice[T] {
	s._mu.Lock()
	defer s._mu.Unlock()

	filtered := []T{}

	for _, item := range s._slice {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}

	s._slice = filtered

	return s
}

// len returns the number of items currently in the slice.
//
// Returns:
//   - The number of items in the slice
func (s *concurrentSlice[T]) len() int {
	s._mu.RLock()
	defer s._mu.RUnlock()

	return len(s._slice)
}

// remove removes the first occurrence of the specified item from the slice.
//
// Parameters:
//   - item: The item to remove from the slice
//
// Returns:
//   - true if the item was found and removed, false otherwise
func (s *concurrentSlice[T]) remove(item T) bool {
	s._mu.Lock()
	defer s._mu.Unlock()

	for i, v := range s._slice {
		if v == item {
			s._slice = append(s._slice[:i], s._slice[i+1:]...)
			return true
		}
	}

	return false
}

func (s *concurrentSlice[T]) toSlice() []T {
	s._mu.RLock()
	defer s._mu.RUnlock()

	items := make([]T, len(s._slice))
	copy(items, s._slice)

	return items
}
