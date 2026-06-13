package pg

type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationRead   OperationType = "READ"
)

type Row map[string]any

func (r Row) PK() PK { return pkFromValue(r["id"]) }

// CDCEvent is the payload of every message produced by PostgresConnector.
// It represents a single row-level change captured from the WAL or snapshot.
type CDCEvent struct {
	// Schema is the Postgres schema name (e.g. "public").
	Schema string
	// Table is the bare table name (e.g. "orders").
	Table string
	// Operation describes the kind of change: INSERT, UPDATE, DELETE, or READ (snapshot).
	Operation OperationType
	// PK is the primary key value of the affected row. Call Value() to obtain the
	// underlying int64 or pg.UUID depending on the column type.
	PK PK
	// Before holds the row state prior to an UPDATE or DELETE. Nil for INSERT and READ.
	Before Row
	// After holds the new row state for INSERT, UPDATE, and READ. Nil for DELETE.
	After Row

	commitXID uint32
	commitLSN uint64
	chunkID   int
}

type Phase string

const (
	PhaseSnapshotting Phase = "snapshotting"
	PhaseStreaming    Phase = "streaming"
)

type CDCState struct {
	Phase           Phase     `json:"phase"`
	SlotName        string    `json:"slot_name"`
	ConfirmedLSN    uint64    `json:"confirmed_lsn"`
	SnapshotTable   string    `json:"snapshot_table"`
	SnapshotCursor  PK        `json:"snapshot_cursor,omitempty"`
	ScannedPKRanges []PKRange `json:"scanned_pk_ranges,omitempty"`
}

type PKRange struct {
	Table string `json:"table"`
	Low   PK     `json:"low"`
	High  PK     `json:"high"`
	XMin  uint32 `json:"xmin"`
}

type walBatch struct {
	events    []CDCEvent
	commitLSN uint64
}
