package pg

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tccpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/ttrung2409/go-broadway/broadway"
)

var testDBDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tccpostgres.Run(ctx, "postgres:16-alpine",
		tccpostgres.WithDatabase("testdb"),
		tccpostgres.WithUsername("test"),
		tccpostgres.WithPassword("test"),
		// WithCmdArgs appends to the module's default "postgres -c fsync=off" cmd
		testcontainers.WithCmdArgs(
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
		),
		// explicit wait: port-only readiness fires before pg accepts connections
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithPollInterval(100*time.Millisecond),
		),
	)
	if err != nil {
		log.Fatalf("could not start postgres container: %v", err)
	}

	testDBDSN = container.MustConnectionString(ctx, "sslmode=disable")

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("could not terminate postgres container: %v", err)
	}
	os.Exit(code)
}

func testDSN() string { return testDBDSN }

type fixture struct {
	conn        *pgx.Conn
	dsn         string
	table       string
	slotName    string
	publication string
}

func newFixture(t *testing.T) *fixture {
	dsn := testDSN()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)

	f := &fixture{
		conn:        conn,
		dsn:         dsn,
		table:       "public.cdc_" + suffix,
		slotName:    "slot_" + suffix,
		publication: "pub_" + suffix,
	}

	_, err = conn.Exec(context.Background(), fmt.Sprintf(
		`CREATE TABLE %s (id BIGSERIAL PRIMARY KEY, data TEXT)`, f.table,
	))
	require.NoError(t, err)

	t.Cleanup(func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, f.table))
		_, _ = conn.Exec(bg, fmt.Sprintf(
			`SELECT pg_drop_replication_slot('%s') WHERE EXISTS `+
				`(SELECT 1 FROM pg_replication_slots WHERE slot_name='%s')`,
			f.slotName, f.slotName,
		))
		_, _ = conn.Exec(bg, fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`, pgQuoteIdent(f.publication)))
		_ = conn.Close(bg)
	})

	return f
}

func (f *fixture) insert(t *testing.T, n int) []int64 {
	t.Helper()
	ids := make([]int64, n)
	for i := range ids {
		err := f.conn.QueryRow(context.Background(),
			fmt.Sprintf(`INSERT INTO %s (data) VALUES ($1) RETURNING id`, f.table),
			fmt.Sprintf("row-%d", i),
		).Scan(&ids[i])
		require.NoError(t, err)
	}
	return ids
}

func (f *fixture) offsetStore(t *testing.T) *PostgresOffsetStore {
	conn, err := pgx.Connect(context.Background(), f.dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	store := NewPostgresOffsetStore(conn, f.slotName)
	require.NoError(t, store.Init(context.Background()))
	return store
}

func (f *fixture) newConnector(chunkSize int, store OffsetStore[CDCState]) *PostgresConnector {
	return New(Config{
		ConnectionString: f.dsn,
		SlotName:         f.slotName,
		Publication:      f.publication,
		Tables:           []string{f.table},
		ChunkSize:        chunkSize,
		OffsetStore:      store,
	})
}

type collectingProcessor struct {
	ch chan CDCEvent
}

func newCollectingProcessor() *collectingProcessor {
	return &collectingProcessor{ch: make(chan CDCEvent, 1000)}
}

func (p *collectingProcessor) Clone() broadway.MessageProcessor { return p }

func (p *collectingProcessor) Handle(
	msg *broadway.Message,
	_ context.Context,
) (*broadway.Message, error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			panic(r)
		}
	}()
	if e, ok := msg.Payload.(CDCEvent); ok {
		p.ch <- e
	}
	return msg, nil
}

func startPipeline(
	ctx context.Context,
	connector *PostgresConnector,
	processor *collectingProcessor,
) *broadway.Pipeline {
	p := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{Producer: connector, Concurrency: 1},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   processor,
			Concurrency: 1,
			MinDemand:   1,
			MaxDemand:   50,
		},
	})
	p.Run(ctx)
	return p
}

func collectN(t *testing.T, ch <-chan CDCEvent, n int) []CDCEvent {
	t.Helper()
	result := make([]CDCEvent, 0, n)
	deadline := time.After(15 * time.Second)
	for len(result) < n {
		select {
		case e := <-ch:
			result = append(result, e)
		case <-deadline:
			t.Fatalf("timeout: got %d/%d events", len(result), n)
		}
	}
	return result
}

func eventIDs(events []CDCEvent) []int64 {
	ids := make([]int64, len(events))
	for i, e := range events {
		ids[i] = e.PK.Value().(int64)
	}
	return ids
}

func TestIntegration_SnapshotOnly(t *testing.T) {
	f := newFixture(t)
	ids := f.insert(t, 20)

	processor := newCollectingProcessor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startPipeline(ctx, f.newConnector(10, nil), processor)
	events := collectN(t, processor.ch, 20)

	for _, e := range events {
		assert.Equal(t, OperationRead, e.Operation)
	}
	assert.ElementsMatch(t, ids, eventIDs(events))
}

func TestIntegration_WALEvents(t *testing.T) {
	f := newFixture(t)

	processor := newCollectingProcessor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startPipeline(ctx, f.newConnector(100, nil), processor)
	time.Sleep(500 * time.Millisecond) // empty snapshot completes quickly

	var id int64
	require.NoError(t, f.conn.QueryRow(context.Background(),
		fmt.Sprintf(`INSERT INTO %s (data) VALUES ('hello') RETURNING id`, f.table),
	).Scan(&id))
	_, err := f.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE %s SET data='world' WHERE id=$1`, f.table), id)
	require.NoError(t, err)
	_, err = f.conn.Exec(context.Background(),
		fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, f.table), id)
	require.NoError(t, err)

	events := collectN(t, processor.ch, 3)

	assert.Equal(t, OperationInsert, events[0].Operation)
	assert.Equal(t, OperationUpdate, events[1].Operation)
	assert.Equal(t, OperationDelete, events[2].Operation)
	for _, e := range events {
		assert.Equal(t, id, e.PK.Value().(int64))
	}
}

func TestIntegration_Concurrent_NoDuplicates(t *testing.T) {
	f := newFixture(t)
	preIDs := f.insert(t, 50)

	processor := newCollectingProcessor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startPipeline(ctx, f.newConnector(10, nil), processor) // 5 chunks for 50 rows

	var (
		mu            sync.Mutex
		concurrentIDs []int64
		wg            sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			time.Sleep(20 * time.Millisecond)
			var id int64
			if f.conn.QueryRow(context.Background(),
				fmt.Sprintf(`INSERT INTO %s (data) VALUES ($1) RETURNING id`, f.table),
				fmt.Sprintf("concurrent-%d", i),
			).Scan(&id) == nil {
				mu.Lock()
				concurrentIDs = append(concurrentIDs, id)
				mu.Unlock()
			}
		}
	}()

	wg.Wait()
	total := len(preIDs) + len(concurrentIDs)
	events := collectN(t, processor.ch, total)

	seen := make(map[int64]int)
	for _, e := range events {
		if e.Operation == OperationRead || e.Operation == OperationInsert {
			seen[e.PK.Value().(int64)]++
		}
	}

	for id, count := range seen {
		assert.Equal(t, 1, count, "id=%d appeared %d times", id, count)
	}
	assert.Equal(t, total, len(seen), "expected %d unique IDs", total)
}

func TestIntegration_ResumeMidSnapshot(t *testing.T) {
	f := newFixture(t)
	f.insert(t, 100)
	store := f.offsetStore(t)

	processor1 := newCollectingProcessor()
	ctx1, cancel1 := context.WithCancel(context.Background())

	pipeline1 := startPipeline(ctx1, f.newConnector(10, store), processor1)
	collectN(t, processor1.ch, 30)     // process 3 chunks
	time.Sleep(200 * time.Millisecond) // allow ACKs to save state
	cancel1()
	<-pipeline1.Terminated() // wait for replication conn to close before second connector starts

	state, found, err := store.Load(context.Background())
	require.NoError(t, err)
	require.True(t, found, "state must be saved after ACK")
	assert.Equal(t, PhaseSnapshotting, state.Phase)
	assert.True(t, state.SnapshotCursor.IsSet(), "snapshot cursor must be set")

	savedCursor := state.SnapshotCursor.Value().(int64)
	assert.Greater(t, savedCursor, int64(0))

	processor2 := newCollectingProcessor()
	ctx2, cancel2 := context.WithCancel(context.Background())

	pipeline2 := startPipeline(ctx2, f.newConnector(10, store), processor2)
	remaining := 100 - int(savedCursor)
	events := collectN(t, processor2.ch, remaining)
	cancel2()
	<-pipeline2.Terminated() // flush ACKs before t.Cleanup closes the store connection

	for _, e := range events {
		assert.Equal(t, OperationRead, e.Operation)
		assert.Greater(t, e.PK.Value().(int64), savedCursor,
			"resumed connector must not re-deliver already-ACKed rows")
	}
}

func TestIntegration_AtLeastOnce_RedeliveryWithoutACK(t *testing.T) {
	f := newFixture(t)
	store := f.offsetStore(t)
	ids := f.insert(t, 5)

	processor1 := newCollectingProcessor()
	ctx1, cancel1 := context.WithCancel(context.Background())

	pipeline1 := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{Producer: f.newConnector(100, store), Concurrency: 1},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   processor1,
			Concurrency: 1,
			MinDemand:   1,
			MaxDemand:   50,
		},
		Acknowledger: func(_ []*broadway.Message, _ error) {}, // withhold ACK
	})
	pipeline1.Run(ctx1)

	collectN(t, processor1.ch, 5)
	cancel1()
	<-pipeline1.Terminated() // wait for replication conn to close before second connector starts

	// ConfirmedLSN was never saved (no-op ACK), so the slot retains WAL for the 5 inserts.
	// The second connector re-delivers them regardless of whether snapshot state was saved.
	processor2 := newCollectingProcessor()
	ctx2, cancel2 := context.WithCancel(context.Background())

	pipeline2 := startPipeline(ctx2, f.newConnector(100, store), processor2)
	events := collectN(t, processor2.ch, 5)
	cancel2()
	<-pipeline2.Terminated() // flush ACKs before t.Cleanup closes the store connection

	assert.ElementsMatch(t, ids, eventIDs(events))
}
