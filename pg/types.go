package pg

import (
	"encoding/json"
	"strconv"
)

type PK struct {
	value *int64
}

func pkAt(v int64) PK      { return PK{value: &v} }
func pkFromValue(v any) PK { return pkAt(toInt64(v)) }
func (p PK) IsSet() bool   { return p.value != nil }
func (p PK) Value() int64  { return *p.value }

func (p PK) Next(n int) PK {
	if p.value == nil {
		return p
	}
	return pkAt(*p.value + int64(n))
}

// inRange reports whether p falls within (low, high].
// An unset low means unbounded start; an unset high means unbounded end.
func (p PK) inRange(low, high PK) bool {
	return (low.value == nil || *p.value > *low.value) &&
		(high.value == nil || *p.value <= *high.value)
}

func (p PK) MarshalJSON() ([]byte, error) {
	if p.value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*p.value)
}

func (p *PK) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		p.value = nil
		return nil
	}
	var v int64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	p.value = &v
	return nil
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationRead   OperationType = "READ"
)

type Row map[string]any

func (r Row) PK() PK { return pkFromValue(r["id"]) }

type CDCEvent struct {
	Schema    string
	Table     string
	Operation OperationType
	PK        PK
	Before    Row
	After     Row

	commitXID uint32
	commitLSN uint64
	chunkID   int
}

type phase string

const (
	PhaseSnapshotting phase = "snapshotting"
	PhaseStreaming    phase = "streaming"
)

type cdcState struct {
	Phase            phase     `json:"phase"`
	SlotName         string    `json:"slot_name"`
	ConfirmedLSN     uint64    `json:"confirmed_lsn"`
	SnapshotTableIdx int       `json:"snapshot_table_idx"`
	SnapshotCursor   PK        `json:"snapshot_cursor,omitempty"`
	ScannedPKRanges  []pkRange `json:"scanned_pk_ranges,omitempty"`
}

type pkRange struct {
	Table string `json:"table"`
	Low   PK     `json:"low"`
	High  PK     `json:"high"`
	XMin  uint32 `json:"xmin"`
}

type walBatch struct {
	events    []CDCEvent
	commitLSN uint64
}
