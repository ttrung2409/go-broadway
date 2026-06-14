package pg

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ttrung2409/go-broadway/broadway"
)

// Config holds all configuration for a PostgresConnector.
type Config struct {
	// ConnectionString is the Postgres connection string, e.g. "postgres://user:pass@host/db".
	ConnectionString string
	// SlotName is the logical replication slot name. Created automatically on first run.
	SlotName string
	// Publication is the Postgres publication name. Created automatically on first run.
	Publication string
	// Tables is the list of tables to capture. Tables are snapshotted sequentially in the order listed.
	// Each entry comprises a schema-qualified table and its primary key column (default "id").
	Tables []CDCTable
	// ChunkSize is the number of rows per snapshot chunk (default: 1000).
	ChunkSize int
	// BufferSize is the internal event buffer capacity between the connector and the pipeline (default: 1000).
	BufferSize int
	// OffsetStore persists CDC state (snapshot cursor, confirmed WAL LSN) across restarts,
	// enabling crash-safe resume. Defaults to PostgresOffsetStore backed by the source database.
	OffsetStore OffsetStore[CDCState]
}

func (c *Config) chunkSizeOrDefault() int {
	if c.ChunkSize <= 0 {
		return 1000
	}
	return c.ChunkSize
}

func (c *Config) bufferSizeOrDefault() int {
	if c.BufferSize <= 0 {
		return 1000
	}
	return c.BufferSize
}

// cdcEngine owns the replication connection, snapshot progress, and WAL offset.
// It is created once and shared across all Connector clones.
type cdcEngine struct {
	mu sync.Mutex

	merger      *merger
	streamer    *walStreamer
	offsetStore OffsetStore[CDCState]
	conn        *pgx.Conn
	state       CDCState

	chunkSizes  map[int]int    // chunkID → expected ACK count
	chunkAcked  map[int]int    // chunkID → received ACK count
	chunkHighPK map[int]PK     // chunkID → high PK of the chunk
	chunkTable  map[int]string // chunkID → schema-qualified table name

	// watermark tracking: cursor only advances past a chunk once all lower-ID chunks are also ACKed
	pendingChunks   map[int]struct{} // chunkIDs seen in HandleDemand but not yet fully ACKed
	completedChunks map[int]PK       // chunkIDs fully ACKed → pkHigh (committed when no lower-ID chunk is pending)

	// broadcast when both pendingChunks and completedChunks become empty
	allChunksAcked *sync.Cond

	snapshotScanDone bool // set to true when all tables have been fully scanned

	done chan struct{} // closed when the WAL streamer exits and the replication connection is released
}

func newCDCEngine() *cdcEngine {
	e := &cdcEngine{
		chunkSizes:      make(map[int]int),
		chunkAcked:      make(map[int]int),
		chunkHighPK:     make(map[int]PK),
		chunkTable:      make(map[int]string),
		pendingChunks:   make(map[int]struct{}),
		completedChunks: make(map[int]PK),
		done:            make(chan struct{}),
	}
	e.allChunksAcked = sync.NewCond(&e.mu)
	return e
}

// PostgresConnector captures all existing rows via incremental snapshotting
// and all subsequent changes via WAL streaming, delivering every change at least once.
type PostgresConnector struct {
	config Config
	engine *cdcEngine
}

// New creates a new PostgresConnector with the given configuration.
func New(config Config) *PostgresConnector {
	return &PostgresConnector{
		config: config,
		engine: newCDCEngine(),
	}
}

// Init is called by the pipeline after instantiation; loads state, opens
// connections, and starts background goroutines.
func (c *PostgresConnector) Init(ctx context.Context) {
	c.start(ctx)
}

// Clone returns a new wrapper sharing the same cdcEngine.
func (c *PostgresConnector) Clone() broadway.Producer {
	return &PostgresConnector{
		config: c.config,
		engine: c.engine,
	}
}

// HandleDemand drains up to demand events from the internal buffer.
// When the context is cancelled it blocks until the WAL streamer has closed
// the replication connection, so the pipeline only terminates after the slot
// is released.
func (c *PostgresConnector) HandleDemand(
	demand int,
	ctx context.Context,
) []*broadway.Message {

	if ctx.Err() != nil {
		<-c.engine.done
		return nil
	}

	e := c.engine
	result := make([]*broadway.Message, 0, demand)
	for i := 0; i < demand; i++ {
		select {
		case event, ok := <-e.merger.output:
			if !ok {
				return result
			}

			// track chunk sizes for snapshot READ events
			if event.Operation == OperationRead {
				table := event.Table
				if event.Schema != "" {
					table = event.Schema + "." + event.Table
				}
				e.mu.Lock()
				e.chunkSizes[event.chunkID] = event.chunkSize
				e.chunkHighPK[event.chunkID] = event.chunkHighPK
				e.chunkTable[event.chunkID] = table
				e.pendingChunks[event.chunkID] = struct{}{}

				e.mu.Unlock()
			}

			result = append(result, broadway.NewMessage(event, event))
		default:
			return result
		}
	}
	return result
}

// Acknowledger advances ConfirmedLSN for WAL
// events and SnapshotCursor when a full chunk is ACKed, then persists the
// updated state so a restart resumes from the correct position.
func (c *PostgresConnector) Acknowledger() broadway.Acknowledger {
	e := c.engine
	return func(messages []*broadway.Message, err error) {
		if err != nil {
			return
		}

		var maxCommitLSN uint64
		chunkACKs := make(map[int]int)

		for _, msg := range messages {
			event, ok := msg.Metadata().(CDCEvent)
			if !ok {
				continue
			}
			if event.Operation == OperationRead {
				chunkACKs[event.chunkID]++
			} else if event.commitLSN > maxCommitLSN {
				maxCommitLSN = event.commitLSN
			}
		}

		e.mu.Lock()
		defer e.mu.Unlock()

		// advance confirmed LSN for WAL events
		if maxCommitLSN > e.state.ConfirmedLSN {
			e.state.ConfirmedLSN = maxCommitLSN
			if e.streamer != nil {
				e.streamer.advanceConfirmedLSN(maxCommitLSN)
			}
		}

		// advance snapshot cursor when all messages in a chunk are ACKed
		advanced := false
		for chunkID, count := range chunkACKs {
			e.chunkAcked[chunkID] += count
			if e.chunkAcked[chunkID] >= e.chunkSizes[chunkID] {
				e.completedChunks[chunkID] = e.chunkHighPK[chunkID]
				delete(e.pendingChunks, chunkID)
				delete(e.chunkSizes, chunkID)
				delete(e.chunkAcked, chunkID)
				delete(e.chunkHighPK, chunkID)
				delete(e.chunkTable, chunkID)
				advanced = true
			}
		}
		if advanced {
			e.advanceSnapshotCursor()
		}

		if e.snapshotScanDone && e.state.Phase == PhaseSnapshotting &&
			len(e.pendingChunks) == 0 && len(e.completedChunks) == 0 {
			e.state.Phase = PhaseStreaming
		}

		if e.offsetStore != nil {
			_ = e.offsetStore.Save(context.Background(), e.state)
		}
	}
}

// advanceSnapshotCursor recomputes the watermark and updates state.SnapshotCursor
// to the highest safe position.
//
// The cursor may only advance to chunk k's pkHigh when every chunk with a lower
// ID has also been fully ACKed — otherwise a crash would leave a gap in delivery.
// completedChunks holds fully-ACKed chunks awaiting commitment; pendingChunks holds
// chunks still in flight. The watermark is the highest-ID completed chunk that has
// no pending predecessor.
func (e *cdcEngine) advanceSnapshotCursor() {
	minPending := int(^uint(0) >> 1) // math.MaxInt without importing math
	for id := range e.pendingChunks {
		if id < minPending {
			minPending = id
		}
	}

	watermarkID := -1
	for id := range e.completedChunks {
		if id < minPending && id > watermarkID {
			watermarkID = id
		}
	}

	if watermarkID >= 0 {
		e.state.SnapshotCursor = e.completedChunks[watermarkID]
		for id := range e.completedChunks {
			if id <= watermarkID {
				delete(e.completedChunks, id)
			}
		}
	}

	if len(e.pendingChunks) == 0 && len(e.completedChunks) == 0 {
		e.allChunksAcked.Broadcast()
	}
}

// start loads state, creates connections, and launches goroutines.
func (c *PostgresConnector) start(ctx context.Context) {
	e := c.engine

	conn, err := pgx.Connect(ctx, c.config.ConnectionString)
	if err != nil {
		panic(fmt.Sprintf("connect: %v", err))
	}

	offsetStore := c.config.OffsetStore
	if offsetStore == nil {
		store := NewPostgresOffsetStore(conn, c.config.SlotName)
		if err := store.Init(ctx); err != nil {
			panic(fmt.Sprintf("init offset store: %v", err))
		}
		offsetStore = store
	}

	state, found, err := offsetStore.Load(ctx)
	if err != nil {
		panic(fmt.Sprintf("load state: %v", err))
	}
	if !found {
		state = CDCState{
			Phase:          PhaseSnapshotting,
			SlotName:       c.config.SlotName,
			SnapshotTables: c.config.Tables,
		}
	}

	var savedRanges []PKRange
	for _, r := range state.ScannedPKRanges {
		savedRanges = append(
			savedRanges,
			PKRange{Table: r.Table, Low: r.Low, High: r.High, XMin: r.XMin},
		)
	}

	m := newMerger(c.config.bufferSizeOrDefault(), savedRanges, state.Phase)

	replConn, err := openReplicationConn(ctx, c.config.ConnectionString)
	if err != nil {
		panic(fmt.Sprintf("open replication conn: %v", err))
	}

	if err := c.ensurePublication(ctx, conn); err != nil {
		panic(fmt.Sprintf("ensure publication: %v", err))
	}

	slotLSN, err := ensureSlot(ctx, replConn, c.config.SlotName)
	if err != nil {
		panic(fmt.Sprintf("ensure slot: %v", err))
	}

	startLSN := pglogrepl.LSN(state.ConfirmedLSN)
	if startLSN == 0 {
		startLSN = slotLSN
	}

	streamer := newWALStreamer(
		replConn,
		c.config.SlotName,
		c.config.Publication,
		startLSN,
		m,
	)

	// initialise engine fields atomically before goroutines start reading them
	e.mu.Lock()
	e.conn = conn
	e.offsetStore = offsetStore
	e.state = state
	e.merger = m
	e.streamer = streamer
	e.chunkSizes = make(map[int]int)
	e.chunkAcked = make(map[int]int)
	e.chunkHighPK = make(map[int]PK)
	e.chunkTable = make(map[int]string)
	e.pendingChunks = make(map[int]struct{})
	e.completedChunks = make(map[int]PK)
	e.done = make(chan struct{})
	e.mu.Unlock()

	go func() {
		defer close(e.done)
		if err := streamer.run(ctx); err != nil && ctx.Err() == nil {
			fmt.Printf("WAL streamer error: %v\n", err)
		}
	}()

	if state.Phase == PhaseSnapshotting {
		snapConn, err := pgx.Connect(ctx, c.config.ConnectionString)
		if err != nil {
			panic(fmt.Sprintf("snapshot connect: %v", err))
		}

		go func() {
			defer func() { _ = snapConn.Close(ctx) }()
			c.runSnapshot(ctx, snapConn, state)
		}()
	}
}

// runSnapshot scans all tables sequentially, resuming from the saved state.
// State persistence is handled solely by the Acknowledger; this function only
// drives the scanner and signals the merger.
func (c *PostgresConnector) runSnapshot(ctx context.Context, conn *pgx.Conn, state CDCState) {
	e := c.engine
	tables := state.SnapshotTables
	chunkID := 1

	startIdx := 0
	for i, t := range tables {
		if t.Name == state.CurrentSnapshotTable {
			startIdx = i
			break
		}
	}

	for i := startIdx; i < len(tables); i++ {
		if ctx.Err() != nil {
			return
		}

		table := tables[i]

		e.mu.Lock()
		if e.state.CurrentSnapshotTable != table.Name {
			e.state.CurrentSnapshotTable = table.Name
			e.state.SnapshotCursor = PK{}
		}
		e.mu.Unlock()

		// resume cursor only for the table we were mid-scan on; fresh tables start unset
		var cursor PK
		if i == startIdx {
			cursor = state.SnapshotCursor
		}

		snapshotter := newSnapshotter(
			conn,
			table.Name,
			table.pkColumn(),
			c.config.chunkSizeOrDefault(),
			e.merger,
		)
		if err := snapshotter.run(ctx, cursor, chunkID); err != nil {
			if ctx.Err() == nil {
				fmt.Printf("snapshot error for %s: %v\n", table, err)
			}

			return
		}

		chunkID += c.config.chunkSizeOrDefault() // rough offset to keep chunk IDs unique across tables
	}

	// Signal that all tables have been scanned. The Acknowledger checks this flag
	// alongside the pending-chunk counts to transition Phase to PhaseStreaming and
	// persist it — keeping state persistence solely in the Acknowledger.
	e.mu.Lock()
	e.snapshotScanDone = true
	e.mu.Unlock()

	// Block until every emitted snapshot chunk has been acknowledged. This ensures that
	// PhaseStreaming is never persisted before all snapshot rows are safely processed:
	// a restart would skip the snapshot, so pre-existing rows not in WAL would be lost.
	go func() {
		<-ctx.Done()
		e.allChunksAcked.Broadcast() // unblock the wait below on cancellation
	}()
	e.mu.Lock()
	for (len(e.pendingChunks) > 0 || len(e.completedChunks) > 0) && ctx.Err() == nil {
		e.allChunksAcked.Wait()
	}
	e.mu.Unlock()
	if ctx.Err() != nil {
		return
	}

	e.merger.onSnapshotComplete()
}

// ensurePublication creates the publication if it does not exist, or alters it
// to exactly match the configured table list if it already exists.
func (c *PostgresConnector) ensurePublication(ctx context.Context, conn *pgx.Conn) error {
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		c.config.Publication,
	).Scan(&exists); err != nil {
		return err
	}

	tables := make([]string, len(c.config.Tables))
	for i, t := range c.config.Tables {
		tables[i] = t.Name
	}

	if exists {
		_, err := conn.Exec(ctx, fmt.Sprintf(
			`ALTER PUBLICATION %s SET TABLE %s`,
			pgQuoteIdent(c.config.Publication),
			strings.Join(tables, ", "),
		))
		return err
	}

	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE PUBLICATION %s FOR TABLE %s`,
		pgQuoteIdent(c.config.Publication),
		strings.Join(tables, ", "),
	))
	return err
}

// openReplicationConn opens a pgconn connection with replication=database.
func openReplicationConn(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	config, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, config)
}

// ensureSlot creates the replication slot if it does not exist and returns its
// confirmed_flush_lsn (0 for a newly created slot).
func ensureSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) (pglogrepl.LSN, error) {
	result, err := pglogrepl.CreateReplicationSlot(
		ctx, conn, slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false},
	)
	if err != nil {
		// slot already exists — that's fine
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "42710" { // duplicate_object
			return 0, nil
		}
		return 0, err
	}
	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	return lsn, err
}
