package pg

import (
	"encoding/json"
	"strconv"
)

type pk struct {
	value *int64
}

func pkAt(v int64) pk      { return pk{value: &v} }
func pkFromValue(v any) pk { return pkAt(toInt64(v)) }
func (p pk) IsSet() bool   { return p.value != nil }
func (p pk) Value() int64  { return *p.value }

func (p pk) Next(n int) pk {
	if p.value == nil {
		return p
	}
	return pkAt(*p.value + int64(n))
}

// inRange reports whether p falls within (low, high].
// An unset low means unbounded start; an unset high means unbounded end.
func (p pk) inRange(low, high pk) bool {
	return (low.value == nil || *p.value > *low.value) &&
		(high.value == nil || *p.value <= *high.value)
}

func (p pk) MarshalJSON() ([]byte, error) {
	if p.value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*p.value)
}

func (p *pk) UnmarshalJSON(b []byte) error {
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

type operationType string

const (
	OperationInsert operationType = "INSERT"
	OperationUpdate operationType = "UPDATE"
	OperationDelete operationType = "DELETE"
	OperationRead   operationType = "READ"
)

type row map[string]any

func (r row) pk() pk { return pkFromValue(r["id"]) }

type cdcEvent struct {
	schema    string
	table     string
	operation operationType
	pk        pk
	before    row
	after     row
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
	SnapshotCursor   pk        `json:"snapshot_cursor,omitempty"`
	ScannedPKRanges  []pkRange `json:"scanned_pk_ranges,omitempty"`
}

type pkRange struct {
	Table string `json:"table"`
	Low   pk     `json:"low"`
	High  pk     `json:"high"`
	XMin  uint32 `json:"xmin"`
}

type walBatch struct {
	events    []cdcEvent
	commitLSN uint64
}
