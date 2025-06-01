package broadway

import (
	"sync"
)

type concurrentMap[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

func newConcurrentMap[K comparable, V any]() *concurrentMap[K, V] {
	return &concurrentMap[K, V]{
		data: make(map[K]V),
	}
}

// Get retrieves a value for the given key
func (m *concurrentMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]
	return val, ok
}

// Set stores a value for the given key
func (m *concurrentMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete removes a key-value pair
func (m *concurrentMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Len returns the number of items in the map
func (m *concurrentMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// ForEach safely iterates over each key-value pair in the map
func (m *concurrentMap[K, V]) ForEach(fn func(K, V) bool) {
	m.mu.RLock()
	snapshot := make(map[K]V)
	for k, v := range m.data {
		snapshot[k] = v
	}
	m.mu.RUnlock()

	for k, v := range snapshot {
		if !fn(k, v) {
			break
		}
	}
}
