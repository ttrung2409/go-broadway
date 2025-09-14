package broadway

import (
	"sync"
)

// concurrentMap is a thread-safe map implementation that allows concurrent access
// from multiple goroutines.
type concurrentMap[K comparable, V any] struct {
	_map map[K]V
	_mu  sync.RWMutex
}

func newConcurrentMap[K comparable, V any](m ...map[K]V) *concurrentMap[K, V] {
	if len(m) == 0 {
		return &concurrentMap[K, V]{
			_map: make(map[K]V),
		}
	}

	cloned := make(map[K]V)
	for k, v := range m[0] {
		cloned[k] = v
	}

	return &concurrentMap[K, V]{
		_map: cloned,
		_mu:  sync.RWMutex{},
	}
}

// get retrieves a value for the given key.
// Returns the value and a boolean flag indicating if the key was found.
// If the key doesn't exist, the zero value of V and false are returned.
func (m *concurrentMap[K, V]) get(key K) (V, bool) {
	m._mu.RLock()
	defer m._mu.RUnlock()
	val, ok := m._map[key]
	return val, ok
}

// set stores a value for the given key.
// If the key already exists, its value will be overwritten.
func (m *concurrentMap[K, V]) set(key K, value V) {
	m._mu.Lock()
	defer m._mu.Unlock()
	m._map[key] = value
}

// delete removes a key-value pair from the map.
// If the key doesn't exist, the operation is a no-op.
func (m *concurrentMap[K, V]) delete(key K) {
	m._mu.Lock()
	defer m._mu.Unlock()
	delete(m._map, key)
}

// len returns the number of items in the map
func (m *concurrentMap[K, V]) len() int {
	m._mu.RLock()
	defer m._mu.RUnlock()
	return len(m._map)
}

func (m *concurrentMap[K, V]) toMap() map[K]V {
	m._mu.RLock()
	defer m._mu.RUnlock()
	snapshot := make(map[K]V)
	for k, v := range m._map {
		snapshot[k] = v
	}

	return snapshot
}

func (m *concurrentMap[K, V]) reset(new map[K]V) {
	m._mu.Lock()
	defer m._mu.Unlock()

	if len(new) == 0 {
		return
	}

	m._map = make(map[K]V, len(new))
	for k, v := range new {
		m._map[k] = v
	}
}

func (m *concurrentMap[K, V]) values() []V {
	m._mu.RLock()
	defer m._mu.RUnlock()

	values := make([]V, 0)
	for _, v := range m._map {
		values = append(values, v)
	}

	return values
}
