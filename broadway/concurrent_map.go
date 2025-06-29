package broadway

import (
	"sync"
)

// concurrentMap is a thread-safe map implementation that allows concurrent access
// from multiple goroutines.
type concurrentMap[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

func newConcurrentMap[K comparable, V any](m ...map[K]V) *concurrentMap[K, V] {
	if len(m) == 0 {
		return &concurrentMap[K, V]{
			data: make(map[K]V),
		}
	}

	cloned := make(map[K]V)
	for k, v := range m[0] {
		cloned[k] = v
	}

	return &concurrentMap[K, V]{
		data: cloned,
		mu:   sync.RWMutex{},
	}
}

// get retrieves a value for the given key.
// Returns the value and a boolean flag indicating if the key was found.
// If the key doesn't exist, the zero value of V and false are returned.
func (m *concurrentMap[K, V]) get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]
	return val, ok
}

// set stores a value for the given key.
// If the key already exists, its value will be overwritten.
func (m *concurrentMap[K, V]) set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// delete removes a key-value pair from the map.
// If the key doesn't exist, the operation is a no-op.
func (m *concurrentMap[K, V]) delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// len returns the number of items in the map
func (m *concurrentMap[K, V]) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *concurrentMap[K, V]) toMap() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := make(map[K]V)
	for k, v := range m.data {
		snapshot[k] = v
	}

	return snapshot
}

func (m *concurrentMap[K, V]) reset(new map[K]V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(new) == 0 {
		return
	}

	m.data = make(map[K]V, len(new))
	for k, v := range new {
		m.data[k] = v
	}
}

func (m *concurrentMap[K, V]) keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]K, 0)
	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}

func (m *concurrentMap[K, V]) values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	values := make([]V, 0)
	for _, v := range m.data {
		values = append(values, v)
	}

	return values
}
