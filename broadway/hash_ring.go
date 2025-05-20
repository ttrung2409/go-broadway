package broadway

import (
	"hash/fnv"
	"sort"
	"sync"
)

type node interface {
	ToString() string
}

type hashRing[N node] struct {
	nodes      []N
	hashToNode map[uint32]N
	mu         sync.RWMutex
}

func newHashRing[N node]() *hashRing[N] {
	return &hashRing[N]{
		nodes:      []N{},
		hashToNode: make(map[uint32]N),
	}
}

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

func (hr *hashRing[N]) GetAllNodes() []N {
	return hr.nodes
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
