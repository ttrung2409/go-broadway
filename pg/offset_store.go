package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type OffsetStore interface {
	Load(ctx context.Context) (cdcState, bool, error)
	Save(ctx context.Context, state cdcState) error
}

// PostgresOffsetStore persists CDC state in a table in the source database.
type PostgresOffsetStore struct {
	conn     *pgx.Conn
	slotName string
}

func NewPostgresOffsetStore(conn *pgx.Conn, slotName string) *PostgresOffsetStore {
	return &PostgresOffsetStore{conn: conn, slotName: slotName}
}

func (s *PostgresOffsetStore) Init(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _pgcdc_offsets (
			slot_name  TEXT PRIMARY KEY,
			state      JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	return err
}

func (s *PostgresOffsetStore) Load(ctx context.Context) (cdcState, bool, error) {
	var raw []byte
	err := s.conn.QueryRow(ctx,
		`SELECT state FROM _pgcdc_offsets WHERE slot_name = $1`,
		s.slotName,
	).Scan(&raw)

	if err == pgx.ErrNoRows {
		return cdcState{}, false, nil
	}
	if err != nil {
		return cdcState{}, false, fmt.Errorf("offset store load: %w", err)
	}

	var state cdcState
	if err := json.Unmarshal(raw, &state); err != nil {
		return cdcState{}, false, fmt.Errorf("offset store unmarshal: %w", err)
	}
	return state, true, nil
}

func (s *PostgresOffsetStore) Save(ctx context.Context, state cdcState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("offset store marshal: %w", err)
	}

	_, err = s.conn.Exec(ctx, `
		INSERT INTO _pgcdc_offsets (slot_name, state)
		VALUES ($1, $2)
		ON CONFLICT (slot_name) DO UPDATE
			SET state = EXCLUDED.state,
			    updated_at = now()
	`, s.slotName, raw)
	return err
}
