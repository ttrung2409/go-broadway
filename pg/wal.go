package pg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const outputPlugin = "pgoutput"

// walStreamer consumes a Postgres logical replication slot, decodes pgoutput
// messages into CDCEvents, and forwards committed transaction batches to the merger.
type walStreamer struct {
	conn        *pgconn.PgConn
	slotName    string
	publication string
	merger      *merger
	relations   map[uint32]*pglogrepl.RelationMessageV2

	confirmedLSN   pglogrepl.LSN
	confirmedLSNCh chan pglogrepl.LSN // receives advances from the acknowledger
}

func newWALStreamer(
	conn *pgconn.PgConn,
	slotName, publication string,
	startLSN pglogrepl.LSN,
	m *merger,
) *walStreamer {
	return &walStreamer{
		conn:           conn,
		slotName:       slotName,
		publication:    publication,
		merger:         m,
		relations:      make(map[uint32]*pglogrepl.RelationMessageV2),
		confirmedLSN:   startLSN,
		confirmedLSNCh: make(chan pglogrepl.LSN, 64),
	}
}

// run starts logical replication from startLSN and streams events until ctx is cancelled.
// The replication connection is closed on return so the slot is released immediately.
func (w *walStreamer) run(ctx context.Context) error {
	defer func() { _ = w.conn.Close(context.Background()) }()
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", w.publication),
		"messages 'true'",
	}

	if err := pglogrepl.StartReplication(
		ctx,
		w.conn,
		w.slotName,
		w.confirmedLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs},
	); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	standbyDeadline := time.Now().Add(10 * time.Second)

	var (
		currentXID uint32
		batch      []CDCEvent
	)

	for {
		select {
		case lsn := <-w.confirmedLSNCh:
			if lsn > w.confirmedLSN {
				w.confirmedLSN = lsn
			}
		default:
		}

		if time.Now().After(standbyDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, w.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: w.confirmedLSN},
			); err != nil {
				return fmt.Errorf("standby status update: %w", err)
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := w.conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		if len(msg.Data) == 0 {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				standbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
			if err != nil {
				return fmt.Errorf("parse logical msg: %w", err)
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				w.relations[m.RelationID] = m

			case *pglogrepl.BeginMessage:
				currentXID = m.Xid
				batch = batch[:0]

			case *pglogrepl.InsertMessageV2:
				rel, ok := w.relations[m.RelationID]
				if !ok {
					break
				}
				event := w.decodeInsert(rel, m, currentXID)
				batch = append(batch, event)

			case *pglogrepl.UpdateMessageV2:
				rel, ok := w.relations[m.RelationID]
				if !ok {
					break
				}
				event := w.decodeUpdate(rel, m, currentXID)
				batch = append(batch, event)

			case *pglogrepl.DeleteMessageV2:
				rel, ok := w.relations[m.RelationID]
				if !ok {
					break
				}
				event := w.decodeDelete(rel, m, currentXID)
				batch = append(batch, event)

			case *pglogrepl.CommitMessage:
				commitLSN := uint64(m.CommitLSN)
				w.merger.onWALBatch(walBatch{events: batch, commitLSN: commitLSN})
				batch = nil
			}
		}
	}
}

// advanceConfirmedLSN notifies the streamer that the given LSN has been
// acknowledged downstream. The streamer sends a StandbyStatusUpdate on the
// next keepalive cycle.
func (w *walStreamer) advanceConfirmedLSN(lsn uint64) {
	select {
	case w.confirmedLSNCh <- pglogrepl.LSN(lsn):
	default:
	}
}

func (w *walStreamer) decodeInsert(
	rel *pglogrepl.RelationMessageV2,
	m *pglogrepl.InsertMessageV2,
	xid uint32,
) CDCEvent {
	after := decodeTuple(rel, m.Tuple)
	return CDCEvent{
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Operation: OperationInsert,
		PK:        extractPKFromRelation(rel, after),
		After:     after,
		commitXID: xid,
	}
}

func (w *walStreamer) decodeUpdate(
	rel *pglogrepl.RelationMessageV2,
	m *pglogrepl.UpdateMessageV2,
	xid uint32,
) CDCEvent {
	after := decodeTuple(rel, m.NewTuple)
	var before Row
	if m.OldTuple != nil {
		before = decodeTuple(rel, m.OldTuple)
	}
	return CDCEvent{
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Operation: OperationUpdate,
		PK:        extractPKFromRelation(rel, after),
		Before:    before,
		After:     after,
		commitXID: xid,
	}
}

func (w *walStreamer) decodeDelete(
	rel *pglogrepl.RelationMessageV2,
	m *pglogrepl.DeleteMessageV2,
	xid uint32,
) CDCEvent {
	before := decodeTuple(rel, m.OldTuple)
	return CDCEvent{
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Operation: OperationDelete,
		PK:        extractPKFromRelation(rel, before),
		Before:    before,
		commitXID: xid,
	}
}

// decodeTuple converts a pglogrepl TupleData into a map of column name → value.
func decodeTuple(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) Row {
	if tuple == nil {
		return nil
	}
	row := make(Row, len(rel.Columns))
	for i, col := range rel.Columns {
		if i >= len(tuple.Columns) {
			break
		}
		tc := tuple.Columns[i]
		switch tc.DataType {
		case 'n':
			row[col.Name] = nil
		case 't':
			row[col.Name] = decodeText(string(tc.Data), col.DataType)
		case 'u':
			// unchanged TOAST — omit
		}
	}
	return row
}

// decodeText converts the Postgres text representation of a value to the
// appropriate Go type based on the column OID.
func decodeText(s string, oid uint32) any {
	switch oid {
	case 16: // bool
		return s == "t" || s == "true" || s == "TRUE"
	case 20, 21, 23, 26: // int8, int2, int4, oid
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	case 700, 701: // float4, float8
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	case 1700: // numeric
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	case 1082, 1114: // date, timestamp
		if v, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
			return v
		}
		if v, err := time.Parse("2006-01-02", s); err == nil {
			return v
		}
	case 1184: // timestamptz
		if v, err := time.Parse("2006-01-02 15:04:05.999999999-07", s); err == nil {
			return v
		}
		if v, err := time.Parse("2006-01-02 15:04:05-07", s); err == nil {
			return v
		}
	}
	return s
}

// extractPKFromRelation returns the PK for the row using the replica identity columns.
func extractPKFromRelation(rel *pglogrepl.RelationMessageV2, row Row) PK {
	for _, col := range rel.Columns {
		if col.Flags == 1 { // part of replica identity
			return pkFromValue(row[col.Name])
		}
	}
	// fallback: use first column
	if len(rel.Columns) > 0 {
		return pkFromValue(row[rel.Columns[0].Name])
	}
	return PK{}
}
