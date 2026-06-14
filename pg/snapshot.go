package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// snapshotter scans a single table in ascending pk order, chunk by chunk,
// emitting READ events through the merger.
type snapshotter struct {
	conn      *pgx.Conn
	table     string // schema-qualified, e.g. "public.orders"
	schema    string // extracted schema name
	tableName string // extracted bare table name
	pkCol     string // primary key column name
	chunkSize int
	merger    *merger
}

func newSnapshotter(conn *pgx.Conn, table, pkCol string, chunkSize int, m *merger) *snapshotter {
	schema, tableName := splitQualifiedTable(table)
	return &snapshotter{
		conn:      conn,
		table:     table,
		schema:    schema,
		tableName: tableName,
		pkCol:     pkCol,
		chunkSize: chunkSize,
		merger:    m,
	}
}

// run scans the table from cursor onwards, signalling the merger for each chunk.
// An unset cursor means start from the beginning of the table.
// Returns when the table is fully scanned.
func (s *snapshotter) run(ctx context.Context, cursor PK, startChunkID int) error {
	chunkID := startChunkID

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		s.merger.onBeginChunk(chunkID, s.table, cursor, cursor.Next(s.chunkSize))

		rows, xmin, actualHigh, err := s.readChunk(ctx, cursor)
		if err != nil {
			return fmt.Errorf("snapshot chunk %d: %w", chunkID, err)
		}

		if len(rows) == 0 {
			return nil
		}

		s.merger.onEndChunk(chunkID, s.table, actualHigh, xmin)

		for _, row := range rows {
			s.merger.onSnapshotEvent(CDCEvent{
				Schema:      s.schema,
				Table:       s.tableName,
				Operation:   OperationRead,
				PK:          pkFromValue(row[s.pkCol]),
				After:       row,
				chunkID:     chunkID,
				chunkSize:   len(rows),
				chunkHighPK: actualHigh,
			})
		}

		cursor = actualHigh
		chunkID++
	}
}

// readChunk executes a single chunk query inside a REPEATABLE READ transaction.
// Returns rows, snapshot xmin, and the actual highest id seen.
func (s *snapshotter) readChunk(ctx context.Context, cursor PK) (
	rows []Row, xmin uint32, high PK, err error,
) {
	tx, err := s.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, 0, PK{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	var snapshotText string
	if err = tx.QueryRow(ctx, `SELECT txid_current_snapshot()`).Scan(&snapshotText); err != nil {
		return nil, 0, PK{}, err
	}
	xmin = parseXMin(snapshotText)

	query, args := s.buildChunkQuery(cursor)
	pgxRows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, PK{}, err
	}
	defer pgxRows.Close()

	fieldDescs := pgxRows.FieldDescriptions()
	colNames := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		colNames[i] = string(fd.Name)
	}

	for pgxRows.Next() {
		vals, scanErr := pgxRows.Values()
		if scanErr != nil {
			return nil, 0, PK{}, scanErr
		}
		row := make(Row, len(colNames))
		for i, col := range colNames {
			row[col] = vals[i]
		}
		rows = append(rows, row)
	}
	if pgxRows.Err() != nil {
		return nil, 0, PK{}, pgxRows.Err()
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, 0, PK{}, err
	}

	if len(rows) > 0 {
		high = pkFromValue(rows[len(rows)-1][s.pkCol])
	}
	return rows, xmin, high, nil
}

func (s *snapshotter) buildChunkQuery(cursor PK) (string, []any) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`SELECT * FROM %s`, s.table))

	quotedPK := pgQuoteIdent(s.pkCol)
	var args []any
	if cursor.IsSet() {
		args = []any{cursor.Value()}
		sb.WriteString(fmt.Sprintf(` WHERE %s > $1`, quotedPK))
	}

	sb.WriteString(fmt.Sprintf(` ORDER BY %s LIMIT %d`, quotedPK, s.chunkSize))
	return sb.String(), args
}

// parseXMin parses the xmin from a txid_current_snapshot() result string.
// Format: "xmin:xmax:xip_list"
func parseXMin(s string) uint32 {
	var xmin uint32
	_, _ = fmt.Sscanf(s, "%d:", &xmin)
	return xmin
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// splitQualifiedTable splits "schema.table" into ("schema", "table").
// If there is no dot, schema is returned as empty string.
func splitQualifiedTable(qualified string) (schema, table string) {
	if idx := strings.IndexByte(qualified, '.'); idx >= 0 {
		return qualified[:idx], qualified[idx+1:]
	}
	return "", qualified
}
