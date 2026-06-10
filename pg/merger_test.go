package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// drain collects all events currently buffered in the merger's output channel.
func drain(m *merger) []cdcEvent {
	var events []cdcEvent
	for {
		select {
		case e := <-m.output:
			events = append(events, e)
		default:
			return events
		}
	}
}

// makeWALEvent builds a minimal WAL-style cdcEvent.
// id is a string because pgoutput tuple columns are decoded as text.
func makeWALEvent(qualifiedTable, id string, xid uint32) cdcEvent {
	schema, table := splitQualifiedTable(qualifiedTable)
	return cdcEvent{
		schema: schema,
		table:     table,
		operation: OperationInsert,
		pk: pkFromValue(id),
		commitXID: xid,
	}
}

func sendWAL(m *merger, event cdcEvent, lsn uint64) {
	m.onWALBatch(walBatch{events: []cdcEvent{event}, commitLSN: lsn})
}

func TestMerger_ScannedRange_BelowXMin_Discarded(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	sendWAL(m, makeWALEvent("public.orders", "500", 850), 1) // XID 850 < xmin 900

	assert.Empty(t, drain(m))
}

func TestMerger_ScannedRange_AboveXMin_Emitted(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	sendWAL(m, makeWALEvent("public.orders", "500", 950), 1) // XID 950 >= xmin 900

	assert.Len(t, drain(m), 1)
}

func TestMerger_ScannedRange_EqualXMin_Emitted(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	sendWAL(m, makeWALEvent("public.orders", "500", 900), 1) // XID 900 == xmin 900

	assert.Len(t, drain(m), 1)
}

func TestMerger_InProgressChunk_AboveXMin_EmittedAfterEnd(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))

	sendWAL(m, makeWALEvent("public.orders", "500", 950), 1)
	assert.Empty(t, drain(m)) // buffered, not yet emitted

	m.onEndChunk(1, "public.orders", pkAt(1000), 900) // xmin=900, 950 >= 900

	assert.Len(t, drain(m), 1)
}

func TestMerger_InProgressChunk_BelowXMin_DiscardedAfterEnd(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))

	sendWAL(m, makeWALEvent("public.orders", "500", 850), 1)
	assert.Empty(t, drain(m))

	m.onEndChunk(1, "public.orders", pkAt(1000), 900) // xmin=900, 850 < 900

	assert.Empty(t, drain(m))
}

func TestMerger_UnscannedPK_Discarded(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	// id=2000 is beyond the scanned range and no chunk is in progress
	sendWAL(m, makeWALEvent("public.orders", "2000", 950), 1)

	assert.Empty(t, drain(m))
}

func TestMerger_DifferentTable_NotMatchedByRange(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	// same id range but different table — not yet scanned, should be discarded
	sendWAL(m, makeWALEvent("public.users", "500", 950), 1)

	assert.Empty(t, drain(m))
}

func TestMerger_SnapshotEvent_AlwaysEmitted(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pkAt(1000))
	m.onEndChunk(1, "public.orders", pkAt(1000), 900)

	m.onSnapshotEvent(cdcEvent{
		schema: "public",
		table:     "orders",
		operation: OperationRead,
		pk: pkFromValue(int64(500)),
		chunkID: 1,
	})

	assert.Len(t, drain(m), 1)
}

func TestMerger_StreamingPhase_AllWALEventsEmitted(t *testing.T) {
	m := newMerger(100, nil, PhaseStreaming)

	m.onWALBatch(walBatch{
		events: []cdcEvent{
			makeWALEvent("public.orders", "1", 100),
			makeWALEvent("public.orders", "2", 101),
		},
		commitLSN: 5,
	})

	assert.Len(t, drain(m), 2)
}

func TestMerger_OnSnapshotComplete_SwitchesToStreaming(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onSnapshotComplete()

	sendWAL(m, makeWALEvent("public.orders", "9999", 999), 1)

	assert.Len(t, drain(m), 1)
}

func TestMerger_CommitLSN_AttachedToEmittedEvent(t *testing.T) {
	m := newMerger(100, nil, PhaseStreaming)

	sendWAL(m, makeWALEvent("public.orders", "1", 100), 42)

	events := drain(m)
	assert.Equal(t, uint64(42), events[0].commitLSN)
}

// The next five tests cover the overflow path: WAL events buffered during a
// chunk whose estimated high is nil (unbounded) but whose actual high is smaller
// than the event's id.  Those events must be carried forward to the chunk that
// actually covers their id, not emitted or discarded using the wrong xmin.

func TestMerger_Overflow_NotEmittedByChunkWithSmallerActualHigh(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)
	m.onBeginChunk(1, "public.orders", pk{}, pk{}) // unbounded estimated high

	sendWAL(m, makeWALEvent("public.orders", "51", 950), 1)
	assert.Empty(t, drain(m)) // buffered

	m.onEndChunk(1, "public.orders", pkAt(10), 900) // actual high=10; id=51 overflows

	assert.Empty(t, drain(m)) // must NOT be emitted here
}

func TestMerger_Overflow_DeduplicatedByLaterChunk_BelowXMin(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)

	// chunk 1: rows 1-10; id=51 XID=150 overflows
	m.onBeginChunk(1, "public.orders", pk{}, pk{})
	sendWAL(m, makeWALEvent("public.orders", "51", 150), 1)
	m.onEndChunk(1, "public.orders", pkAt(10), 100)
	assert.Empty(t, drain(m))

	// chunk 6 covers rows 51-60; XID=150 < xmin=200 → snapshot captured it → discard
	m.onBeginChunk(6, "public.orders", pkAt(50), pkAt(60))
	m.onEndChunk(6, "public.orders", pkAt(60), 200)

	assert.Empty(t, drain(m))
}

func TestMerger_Overflow_DeduplicatedByLaterChunk_AboveXMin(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)

	// chunk 1: rows 1-10; id=51 XID=250 overflows
	m.onBeginChunk(1, "public.orders", pk{}, pk{})
	sendWAL(m, makeWALEvent("public.orders", "51", 250), 1)
	m.onEndChunk(1, "public.orders", pkAt(10), 100)
	assert.Empty(t, drain(m))

	// chunk 6 covers rows 51-60; XID=250 >= xmin=200 → snapshot missed it → emit
	m.onBeginChunk(6, "public.orders", pkAt(50), pkAt(60))
	m.onEndChunk(6, "public.orders", pkAt(60), 200)

	assert.Len(t, drain(m), 1)
}

func TestMerger_Overflow_EmittedOnSnapshotComplete(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)

	// chunk 1 overflow; snapshot ends without ever scanning id=51
	m.onBeginChunk(1, "public.orders", pk{}, pk{})
	sendWAL(m, makeWALEvent("public.orders", "51", 200), 1)
	m.onEndChunk(1, "public.orders", pkAt(10), 100)
	assert.Empty(t, drain(m))

	m.onSnapshotComplete()

	assert.Len(t, drain(m), 1)
}

func TestMerger_NoDuplicate_OverflowDiscardedWhenSnapshotCapturedRow(t *testing.T) {
	m := newMerger(100, nil, PhaseSnapshotting)

	// chunk 1 overflow for id=51 (XID=150 < xmin6=200 → snapshot captured it)
	m.onBeginChunk(1, "public.orders", pk{}, pk{})
	sendWAL(m, makeWALEvent("public.orders", "51", 150), 1)
	m.onEndChunk(1, "public.orders", pkAt(10), 100)

	m.onBeginChunk(6, "public.orders", pkAt(50), pkAt(60))
	m.onEndChunk(6, "public.orders", pkAt(60), 200) // overflow discarded

	m.onSnapshotEvent(cdcEvent{ // snapshot emits READ for id=51
		schema: "public", table: "orders", operation: OperationRead,
		pk: pkFromValue(int64(51)), chunkID: 6,
	})

	events := drain(m)
	assert.Len(t, events, 1)
	assert.Equal(t, OperationRead, events[0].operation) // READ only, no duplicate INSERT
}

func TestMerger_RestoredRanges_DeduplicateCorrectly(t *testing.T) {
	saved := []pkRange{{Table: "public.orders", Low: pk{}, High: pkAt(1000), XMin: 900}}
	m := newMerger(100, saved, PhaseSnapshotting)

	// already-scanned range: XID below xmin → discarded
	sendWAL(m, makeWALEvent("public.orders", "500", 850), 1)
	assert.Empty(t, drain(m))

	// already-scanned range: XID above xmin → emitted
	sendWAL(m, makeWALEvent("public.orders", "500", 950), 2)
	assert.Len(t, drain(m), 1)
}
