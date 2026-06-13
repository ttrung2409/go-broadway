package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newEngineForWatermark(pending []int, completed map[int]int64) *cdcEngine {
	e := newCDCEngine()
	for _, id := range pending {
		e.pendingChunks[id] = struct{}{}
	}
	for id, pk := range completed {
		e.completedChunks[id] = intPK(pk)
	}
	return e
}

func intPK(v int64) PK { return pkFromValue(v) }

func cursorValue(e *cdcEngine) int64 {
	if !e.state.SnapshotCursor.IsSet() {
		return 0
	}
	return e.state.SnapshotCursor.Value().(int64)
}

func TestWatermark_NoPendingNoCompleted_NoAdvance(t *testing.T) {
	e := newEngineForWatermark(nil, nil)
	e.advanceSnapshotCursor()
	assert.False(t, e.state.SnapshotCursor.IsSet())
}

func TestWatermark_AllChunksPending_NoAdvance(t *testing.T) {
	e := newEngineForWatermark([]int{1, 2, 3}, nil)
	e.advanceSnapshotCursor()
	assert.False(t, e.state.SnapshotCursor.IsSet())
}

func TestWatermark_MinChunkACKedFirst_AdvancesToIt(t *testing.T) {
	// chunks 2 and 3 still pending; chunk 1 just completed
	e := newEngineForWatermark([]int{2, 3}, map[int]int64{1: 10})
	e.advanceSnapshotCursor()
	assert.EqualValues(t, 10, cursorValue(e))
	assert.Empty(t, e.completedChunks)
}

func TestWatermark_NonMinACKedFirst_NoAdvance(t *testing.T) {
	// chunk 3 ACKed before chunk 1 and 2 — cursor must not jump
	e := newEngineForWatermark([]int{1, 2}, map[int]int64{3: 30})
	e.advanceSnapshotCursor()
	assert.False(t, e.state.SnapshotCursor.IsSet())
	assert.Len(t, e.completedChunks, 1) // chunk 3 still waiting
}

func TestWatermark_Cascade_AdvancesPastMultiple(t *testing.T) {
	// chunk 2 just ACKed; chunks 1 and 3 already completed, chunk 4 still pending
	// expected: cursor jumps straight to pkHigh(3)
	e := newEngineForWatermark([]int{4}, map[int]int64{1: 10, 2: 20, 3: 30})
	e.advanceSnapshotCursor()
	assert.EqualValues(t, 30, cursorValue(e))
	assert.Empty(t, e.completedChunks)
}

func TestWatermark_GapInCompleted_StopsBeforeGap(t *testing.T) {
	// chunks 1 and 3 ACKed, chunk 2 still pending — cursor can only reach pkHigh(1)
	e := newEngineForWatermark([]int{2}, map[int]int64{1: 10, 3: 30})
	e.advanceSnapshotCursor()
	assert.EqualValues(t, 10, cursorValue(e))
	assert.Len(t, e.completedChunks, 1) // chunk 3 still waiting
	_, stillHas3 := e.completedChunks[3]
	assert.True(t, stillHas3)
}

func TestWatermark_AllPendingGone_AdvancesToMax(t *testing.T) {
	// last in-flight chunk just ACKed — all done
	e := newEngineForWatermark(nil, map[int]int64{1: 10, 2: 20, 3: 30})
	e.advanceSnapshotCursor()
	assert.EqualValues(t, 30, cursorValue(e))
	assert.Empty(t, e.completedChunks)
}
