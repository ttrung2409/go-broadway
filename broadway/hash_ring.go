package broadway

import (
	"hash/fnv"
	"sort"
	"sync"
)

// node is an interface that defines the minimal requirements for objects
// that can be added to a hash ring. Any type implementing this interface
// can be used as a node in the hash ring.
type node interface {
	// toString returns a string representation of the node that is used
	// for generating a consistent hash for the node.
	toString() string
}

// hashRing is a generic implementation of a consistent hash ring.
// It is used to distribute load across multiple nodes while ensuring
// that when nodes are added or removed, the distribution changes minimally.
//
// Type parameter N must satisfy the node interface.
type hashRing[N node] struct {
	_nodes      []N          // Sorted list of nodes in the ring
	_hashToNode map[uint32]N // Mapping from hash values to nodes
	_mu         sync.RWMutex // Lock for thread safety
}

func newHashRing[N node]() *hashRing[N] {
	return &hashRing[N]{
		_nodes:      []N{},
		_hashToNode: make(map[uint32]N),
	}
}

// addNode adds a node to the hash ring.
// If a node with the same hash already exists, the operation is ignored.
//
// Parameters:
//   - node: The node to be added to the hash ring
func (hr *hashRing[N]) addNode(node N) {
	hr._mu.Lock()
	defer hr._mu.Unlock()

	hash := hashString(node.toString())
	if _, exists := hr._hashToNode[hash]; exists {
		return
	}

	hr._nodes = append(hr._nodes, node)
	hr._hashToNode[hash] = node
	sort.Slice(hr._nodes, func(i, j int) bool {
		return hashString(hr._nodes[i].toString()) < hashString(hr._nodes[j].toString())
	})
}

// RemoveNode removes a node from the hash ring.
// If the node doesn't exist in the ring, the operation is ignored.

// Parameters:
//   - node: The node to remove from the hash ring
func (hr *hashRing[N]) removeNode(node N) {
	hr._mu.Lock()
	defer hr._mu.Unlock()

	hash := hashString(node.toString())
	if _, exists := hr._hashToNode[hash]; !exists {
		return
	}

	delete(hr._hashToNode, hash)

	for i, n := range hr._nodes {
		if n.toString() == node.toString() {
			hr._nodes = append(hr._nodes[:i], hr._nodes[i+1:]...)
			break
		}
	}
}

// getNode returns the node responsible for the given key.
// This is the core function of the consistent hashing algorithm.
// It finds the node with the smallest hash that is greater than or equal to the key's hash.
// If no such node exists, it wraps around to the first node in the ring.
//
// Parameters:
//   - key: The key for which to find the responsible node
//
// Returns:
//   - The node responsible for the key
//   - true if a node was found, false otherwise
func (hr *hashRing[N]) getNode(key string) (N, bool) {
	hr._mu.RLock()
	defer hr._mu.RUnlock()

	if len(hr._nodes) == 0 {
		var zero N
		return zero, false
	}

	hash := hashString(key)
	for _, node := range hr._nodes {
		if hashString(node.toString()) >= hash {
			return node, true
		}
	}

	// Wrap around to the first node
	return hr._nodes[0], true
}

// getNextNode returns the next node in the hash ring after the given node.
//
// Parameters:
//   - node: The node for which to find the next node in the ring
//
// Returns:
//   - The next node in the hash ring
//   - true if a next node was found, false otherwise
func (hr *hashRing[N]) getNextNode(node N) (N, bool) {
	hr._mu.RLock()
	defer hr._mu.RUnlock()

	if len(hr._nodes) == 0 {
		var zero N
		return zero, false
	}

	hash := hashString(node.toString())

	i := sort.Search(len(hr._nodes), func(i int) bool {
		return hashString(hr._nodes[i].toString()) > hash
	})

	if i == len(hr._nodes) {
		return hr._nodes[0], true
	}

	return hr._nodes[i], true
}

// getAllNodes returns all nodes currently in the hash ring.
//
// Returns:
//   - A slice containing all nodes in the hash ring
func (hr *hashRing[N]) getAllNodes() []N {
	hr._mu.RLock()
	defer hr._mu.RUnlock()

	return hr._nodes
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
