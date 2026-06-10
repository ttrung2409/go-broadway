package pg

import (
	"sync"
)

// merger receives snapshot READ events and WAL event batches from separate goroutines,
// deduplicates them using per-chunk XID boundaries, and writes the merged stream
// to an output channel that HandleDemand drains.
type merger struct {
	mu sync.Mutex

	phase            phase
	scannedRanges    []pkRange
	currentChunk     *chunk
	pendingWALEvents []CDCEvent

	output chan CDCEvent
}

type chunk struct {
	chunkID int
	table   string
	pkLow   PK
	pkHigh  PK
}

func newMerger(bufferSize int, initialRanges []pkRange, phase phase) *merger {
	return &merger{
		phase:         phase,
		scannedRanges: initialRanges,
		output:        make(chan CDCEvent, bufferSize),
	}
}

// onBeginChunk is called by the scanner before executing a chunk query.
// pkLow/pkHigh are the estimated id bounds; the merger buffers WAL events in this range.
func (m *merger) onBeginChunk(chunkID int, table string, pkLow, pkHigh PK) {
	m.mu.Lock()
	m.currentChunk = &chunk{
		chunkID: chunkID,
		table:   table,
		pkLow:   pkLow,
		pkHigh:  pkHigh,
	}
	m.mu.Unlock()
}

// onEndChunk is called after the chunk query commits and xmin is known.
// Pending WAL events whose id falls within [pkLow, actualIDHigh] are
// deduplicated against xmin and emitted or discarded.  Events outside that
// range are carried forward as overflow: they will be re-evaluated by the
// chunk that actually covers their id, preventing the snapshot from emitting
// a READ and the overflow from also emitting an INSERT for the same row.
func (m *merger) onEndChunk(chunkID int, table string, actualIDHigh PK, xmin uint32) {
	m.mu.Lock()

	if m.currentChunk == nil || m.currentChunk.chunkID != chunkID {
		m.mu.Unlock()
		return
	}

	pkLow := m.currentChunk.pkLow
	m.currentChunk = nil

	completed := pkRange{Table: table, Low: pkLow, High: actualIDHigh, XMin: xmin}
	m.scannedRanges = append(m.scannedRanges, completed)

	pending := m.pendingWALEvents
	m.pendingWALEvents = nil

	var toEmit []CDCEvent
	for _, event := range pending {
		if event.PK.inRange(pkLow, actualIDHigh) {
			if event.commitXID >= xmin {
				toEmit = append(toEmit, event)
			}
		} else {
			// outside the actual range: carry forward for the chunk that will scan this id
			m.pendingWALEvents = append(m.pendingWALEvents, event)
		}
	}

	m.mu.Unlock()

	for _, event := range toEmit {
		m.output <- event
	}
}

// onSnapshotComplete switches the merger to pass-through mode.
// Any remaining overflow events were never captured by the snapshot (their id
// is beyond the last scanned row), so they are emitted directly here.
func (m *merger) onSnapshotComplete() {
	m.mu.Lock()
	overflow := m.pendingWALEvents
	m.phase = PhaseStreaming
	m.currentChunk = nil
	m.pendingWALEvents = nil
	m.scannedRanges = nil
	m.mu.Unlock()

	for _, event := range overflow {
		m.output <- event
	}
}

// onSnapshotEvent routes a READ event from the scanner directly to output.
func (m *merger) onSnapshotEvent(event CDCEvent) {
	m.output <- event
}

// onWALBatch routes each event in a committed transaction batch.
func (m *merger) onWALBatch(batch walBatch) {
	m.mu.Lock()
	toEmit := m.collectWALBatch(batch)
	m.mu.Unlock()

	for _, event := range toEmit {
		m.output <- event
	}
}

// collectWALBatch applies routing/deduplication for a batch, returning events to emit.
func (m *merger) collectWALBatch(batch walBatch) []CDCEvent {
	var toEmit []CDCEvent
	for _, event := range batch.events {
		event.commitLSN = batch.commitLSN
		if e, ok := m.routeEvent(event); ok {
			toEmit = append(toEmit, e)
		}
	}
	return toEmit
}

// routeEvent returns the event and whether it should be emitted.
// Buffers the event in pendingWALEvents or discards it when appropriate.
func (m *merger) routeEvent(event CDCEvent) (CDCEvent, bool) {
	if m.phase == PhaseStreaming {
		return event, true
	}

	qualifiedTable := qualifyTable(event.Schema, event.Table)

	if r := findScannedRange(m.scannedRanges, qualifiedTable, event.PK); r != nil {
		return event, event.commitXID >= r.XMin
	}

	if m.currentChunk != nil &&
		m.currentChunk.table == qualifiedTable &&
		event.PK.inRange(m.currentChunk.pkLow, m.currentChunk.pkHigh) {
		m.pendingWALEvents = append(m.pendingWALEvents, event)
		return event, false
	}

	// not yet scanned — snapshot will capture current state; discard
	return event, false
}

func qualifyTable(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func findScannedRange(ranges []pkRange, table string, id PK) *pkRange {
	for i := range ranges {
		r := &ranges[i]
		if r.Table == table && id.inRange(r.Low, r.High) {
			return r
		}
	}
	return nil
}
