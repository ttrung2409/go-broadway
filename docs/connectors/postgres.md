# PostgresConnector

`PostgresConnector` (package `pg`) captures a Postgres database as a CDC stream. It first snapshots all existing rows in ascending PK order, then switches to streaming live changes via WAL logical replication — delivering every change at least once with crash-safe resume.

---

## Prerequisites

```sql
wal_level = logical
```

The connector creates the replication slot and publication automatically on first run. All captured tables must have a single-column primary key named `id`. Both `bigint`/`bigserial` and `uuid` column types are supported.

---

## Handling CDC events

The message payload is a `pg.CDCEvent`. Use `pg.ParseRow` to decode the row into a typed struct, then switch on the operation:

```go
type Order struct {
    ID    int64  `json:"id"`
    Total int    `json:"total"`
}

func (p *MyCDCProcessor) Handle(msg *broadway.Message, ctx context.Context) (*broadway.Message, error) {
    event := msg.Payload.(pg.CDCEvent)

    switch event.Operation {
    case pg.OperationRead, pg.OperationInsert:
        order, err := pg.ParseRow[Order](event.After)
    case pg.OperationUpdate:
        before, err := pg.ParseRow[Order](event.Before)
        after, err  := pg.ParseRow[Order](event.After)
    case pg.OperationDelete:
        order, err := pg.ParseRow[Order](event.Before)
    }

    return msg, nil
}
```

---

## Offset store and At-least-once delivery

Offsets are persisted after every acknowledged batch. On restart the connector resumes from the last confirmed position — snapshot chunks that were not fully acknowledged are re-scanned, and WAL re-delivers from the last confirmed LSN.

By default the connector persists offsets to a `_pgcdc_offsets` table in the source database via `PostgresOffsetStore`. Pass a custom implementation to store offsets elsewhere — for example, in Redis or a separate database:

```go
connector := pg.New(pg.Config{
    // ...
    OffsetStore: &MyOffsetStore{},
})
```

`Load` is called once on startup to resume from the last saved position. `Save` is called after every acknowledged batch — keep it fast and idempotent.

---

## Configuration reference

| Field | Type | Default | Description |
|---|---|---|---|
| `ConnectionString` | `string` | — | Postgres connection string |
| `SlotName` | `string` | — | Replication slot name (created if absent) |
| `Publication` | `string` | — | Publication name (created if absent) |
| `Tables` | `[]string` | — | Schema-qualified table names to capture |
| `ChunkSize` | `int` | `1000` | Rows per snapshot chunk |
| `BufferSize` | `int` | `10000` | Internal event buffer capacity |
| `OffsetStore` | `OffsetStore` | Postgres | Where to persist CDC offsets |

