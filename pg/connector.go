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
	// Tables is a list of schema-qualified table names to capture, e.g. "public.orders".
	// Tables are snapshotted sequentially in the order listed.
	// All captured tables must have a single-column primary key named "id" (bigint or uuid).
	Tables []string
	// ChunkSize is the number of rows per snapshot chunk (default: 1000).
	ChunkSize int
	// BufferSize is the internal event buffer capacity between the connector and the pipeline (default: 10000).
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
		return 10000
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
}

func newCDCEngine() *cdcEngine {
	return &cdcEngine{
		chunkSizes:  make(map[int]int),
		chunkAcked:  make(map[int]int),
		chunkHighPK: make(map[int]PK),
		chunkTable:  make(map[int]string),
	}
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
func (c *PostgresConnector) HandleDemand(
	demand int,
	ctx context.Context,
) []*broadway.Message {

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
				e.chunkSizes[event.chunkID]++
				e.chunkHighPK[event.chunkID] = event.PK
				e.chunkTable[event.chunkID] = table
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
		for chunkID, count := range chunkACKs {
			e.chunkAcked[chunkID] += count
			if e.chunkAcked[chunkID] >= e.chunkSizes[chunkID] && e.chunkSizes[chunkID] > 0 {
				if pkHigh := e.chunkHighPK[chunkID]; pkHigh.IsSet() {
					e.state.SnapshotCursor = pkHigh
				}

				delete(e.chunkSizes, chunkID)
				delete(e.chunkAcked, chunkID)
				delete(e.chunkHighPK, chunkID)
				delete(e.chunkTable, chunkID)
			}
		}

		if e.offsetStore != nil {
			_ = e.offsetStore.Save(context.Background(), e.state)
		}
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
			Phase:    PhaseSnapshotting,
			SlotName: c.config.SlotName,
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
	e.mu.Unlock()

	go func() {
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
	tables := c.config.Tables
	chunkID := 1

	startIdx := 0
	for i, t := range tables {
		if t == state.SnapshotTable {
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
		if e.state.SnapshotTable != table {
			e.state.SnapshotTable = table
			e.state.SnapshotCursor = PK{}
		}
		e.mu.Unlock()

		// resume cursor only for the table we were mid-scan on; fresh tables start unset
		var cursor PK
		if i == startIdx {
			cursor = state.SnapshotCursor
		}

		snapshotter := newSnapshotter(conn, table, c.config.chunkSizeOrDefault(), e.merger)
		if err := snapshotter.run(ctx, cursor, chunkID); err != nil {
			if ctx.Err() == nil {
				fmt.Printf("snapshot error for %s: %v\n", table, err)
			}
			return
		}

		chunkID += c.config.chunkSizeOrDefault() // rough offset to keep chunk IDs unique across tables
	}

	// switch merger to pass-through
	e.merger.onSnapshotComplete()

	e.mu.Lock()
	e.state.Phase = PhaseStreaming
	if e.offsetStore != nil {
		_ = e.offsetStore.Save(context.Background(), e.state)
	}
	e.mu.Unlock()
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
	copy(tables, c.config.Tables)

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
