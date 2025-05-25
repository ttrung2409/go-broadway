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
	// ToString returns a string representation of the node that is used
	// for generating a consistent hash for the node.
	ToString() string
}

// hashRing is a generic implementation of a consistent hash ring.
// It is used to distribute load across multiple nodes while ensuring
// that when nodes are added or removed, the distribution changes minimally.
//
// Type parameter N must satisfy the node interface.
type hashRing[N node] struct {
	nodes      []N          // Sorted list of nodes in the ring
	hashToNode map[uint32]N // Mapping from hash values to nodes
	mu         sync.RWMutex // Lock for thread safety
}

func newHashRing[N node]() *hashRing[N] {
	return &hashRing[N]{
		nodes:      []N{},
		hashToNode: make(map[uint32]N),
	}
}

// AddNode adds a node to the hash ring.
// If a node with the same hash already exists, the operation is ignored.
//
// Parameters:
//   - node: The node to be added to the hash ring
func (hr *hashRing[N]) AddNode(node N) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hash := hashString(node.ToString())
	if _, exists := hr.hashToNode[hash]; exists {
		return
	}

	hr.nodes = append(hr.nodes, node)
	hr.hashToNode[hash] = node
	sort.Slice(hr.nodes, func(i, j int) bool {
		return hashString(hr.nodes[i].ToString()) < hashString(hr.nodes[j].ToString())
	})
}

// RemoveNode removes a node from the hash ring.
// If the node doesn't exist in the ring, the operation is ignored.

// Parameters:
//   - node: The node to remove from the hash ring
func (hr *hashRing[N]) RemoveNode(node N) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hash := hashString(node.ToString())
	if _, exists := hr.hashToNode[hash]; !exists {
		return
	}

	delete(hr.hashToNode, hash)

	for i, n := range hr.nodes {
		if n.ToString() == node.ToString() {
			hr.nodes = append(hr.nodes[:i], hr.nodes[i+1:]...)
			break
		}
	}
}

// GetNode returns the node responsible for the given key.
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
func (hr *hashRing[N]) GetNode(key string) (N, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.nodes) == 0 {
		var zero N
		return zero, false
	}

	hash := hashString(key)
	for _, node := range hr.nodes {
		if hashString(node.ToString()) >= hash {
			return node, true
		}
	}

	// Wrap around to the first node
	return hr.nodes[0], true
}

// GetNextNode returns the next node in the hash ring after the given node.
//
// Parameters:
//   - node: The node for which to find the next node in the ring
//
// Returns:
//   - The next node in the hash ring
//   - true if a next node was found, false otherwise
func (hr *hashRing[N]) GetNextNode(node N) (N, bool) {
	if len(hr.nodes) == 0 {
		var zero N
		return zero, false
	}

	hash := hashString(node.ToString())

	i := sort.Search(len(hr.nodes), func(i int) bool {
		return hashString(hr.nodes[i].ToString()) > hash
	})

	if i == len(hr.nodes) {
		return hr.nodes[0], true
	}

	return hr.nodes[i], true
}

// GetPrevNode returns the previous node in the hash ring before the given node.
//
// Parameters:
//   - node: The node for which to find the previous node in the ring
//
// Returns:
//   - The previous node in the hash ring
//   - true if a previous node was found, false otherwise
func (hr *hashRing[N]) GetPrevNode(node N) (N, bool) {
	if len(hr.nodes) == 0 {
		var zero N
		return zero, false
	}

	hash := hashString(node.ToString())

	i := sort.Search(len(hr.nodes), func(i int) bool {
		return hashString(hr.nodes[i].ToString()) >= hash
	})

	if i == 0 {
		return hr.nodes[len(hr.nodes)-1], true
	}

	return hr.nodes[i-1], true
}

// GetAllNodes returns all nodes currently in the hash ring.
//
// Returns:
//   - A slice containing all nodes in the hash ring
func (hr *hashRing[N]) GetAllNodes() []N {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.nodes
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
