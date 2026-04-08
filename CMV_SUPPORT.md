# CMV (Continuous Materialized View) Support for little_bigtable

## Overview

Bigtable CMVs allow you to re-key table data for efficient queries on alternate key orderings.
Production Bigtable handles CMV maintenance automatically. This emulator replicates that
behavior via the standard `CreateMaterializedView` gRPC method.

## How It Works

### 1. Creating a CMV

Use the standard Go admin client, pointed at the emulator:

```go
iac, err := bigtable.NewInstanceAdminClient(ctx, project)
err = iac.CreateMaterializedView(ctx, instanceID, &bigtable.MaterializedViewInfo{
    MaterializedViewID: "events_by_account",
    Query: `SELECT
  SPLIT(_key, '#')[SAFE_OFFSET(3)] AS region,
  SPLIT(_key, '#')[SAFE_OFFSET(4)] AS account_id,
  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS ts,
  SPLIT(_key, '#')[SAFE_OFFSET(2)] AS typ,
  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS item_id,
  _key AS src_key,
  cf1 AS cf1
FROM ` + "`events`" + `
ORDER BY region, account_id, ts, typ, item_id, src_key`,
})
```

The emulator parses the SQL to extract the key transformation config. The same code works
against both production Bigtable and the emulator.

### 2. Write-time Sync

When data is written to a source table (via MutateRow, MutateRows, CheckAndMutateRow,
or ReadModifyWriteRow), the emulator automatically:

1. Detects if the target table has any registered CMVs
2. Creates the CMV shadow table (if it doesn't exist yet) with matching column families
3. Transforms the source row key per the SQL's `ORDER BY`
4. Writes the re-keyed row to the shadow table

### 3. Delete Propagation

When source rows are deleted (DeleteFromRow mutation, DropRowRange), the emulator
derives the CMV key and deletes the corresponding CMV row.

### 4. Reading from the CMV

Since the CMV shadow table is a regular table, reads use the standard approach:

```go
table := client.Open("events_by_account")
row, err := table.ReadRow(ctx, "region-a#account-42#...")
```

## What's Changed

### New Files
- `bttest/cmv.go` — CMV config types, registry, key transformation logic
- `bttest/sql_parse.go` — SQL parser for extracting CMV config from a `CreateMaterializedView` query
- `bttest/cmv_test.go` — Tests for key transformation, write sync, delete propagation
- `bttest/sql_parse_test.go` — Tests for the SQL parser

### Modified Files
- `little_bigtable.go` — Version bump to 0.2.0
- `bttest/inmem.go` — Added `cmvs` field to server struct, CMV registration,
  shadow table creation, write-time sync hooks in MutateRow/MutateRows/
  CheckAndMutateRow/ReadModifyWriteRow/DropRowRange
- `bttest/instance_server.go` — Implemented CreateMaterializedView, GetMaterializedView,
  ListMaterializedViews, UpdateMaterializedView (DeletionProtection only), DeleteMaterializedView

## Known Limitations

- **SQL parser**: CMV SQL is parsed with regex scoped to the standard Bigtable CMV format.
  Unusual SQL formatting may fail to parse.
- **GC policy propagation**: The CMV shadow table copies column families from the source
  at creation time. If the source table's GC policies change later, the CMV won't update.
- **ModifyColumnFamilies sync**: Column family changes on the source table after CMV
  creation are not reflected in the CMV table.
- **Backfill**: Data written to the source table before the CMV is registered is not
  retroactively copied.
- **Persistence**: CMV registrations are persisted to SQLite alongside table data and are
  automatically restored on startup. Shadow table row data is also persistent (it is stored
  as a regular table in `tables_t`).

## Example: Key Transformation

Source row key format (5 components):
```
item_id#timestamp#type#region#account_id
```

With `ORDER BY region, account_id, timestamp, type, item_id, src_key`, a source key:
```
item-abc#9999999#type-x#region-a#account-42
```
Becomes CMV key:
```
region-a#account-42#9999999#type-x#item-abc#item-abc#9999999#type-x#region-a#account-42
```

The first 5 components are the re-ordered key; the remainder is the full original source
key appended because `_key AS src_key` appears in the `ORDER BY`.
