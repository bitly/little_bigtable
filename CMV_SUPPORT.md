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

Open a view with `client.OpenMaterializedView`. The emulator stores a view's rows in an internal
shadow table, but like prod Bigtable it isn't a client-facing table: requests that address it by
table name (`client.Open`) return `NotFound`, and it is hidden from `ListTables`. A view that has
had no writes reads as empty. View row keys
use prod Bigtable's encoding (see [Example: Key Transformation](#example-key-transformation)):
each key column is terminated with `\x00\x01`, so do not build lookup keys by joining
columns with `#`.

```go
// Key columns in ORDER BY order, each terminated with \x00\x01 except the last.
// This view's last key column is src_key, the full source row key.
key := "region-a\x00\x01account-42\x00\x019999999\x00\x01type-x\x00\x01item-abc\x00\x01" +
    "item-abc#9999999#type-x#region-a#account-42"

table := client.OpenMaterializedView("events_by_account")
row, err := table.ReadRow(ctx, key)
```

To read every row for an account, scan a prefix of the leading key columns. Keep the trailing
`\x00\x01` so `account-42` does not also match `account-421`:

```go
prefix := "region-a\x00\x01account-42\x00\x01"
err := table.ReadRows(ctx, bigtable.PrefixRange(prefix), func(row bigtable.Row) bool {
    // row.Key() is the full encoded view key
    return true
})
```

A small helper keeps call sites readable. Columns that can contain a `\x00` byte must have it
escaped as `\x00\xff`:

```go
func cmvKey(cols ...string) string {
    esc := make([]string, len(cols))
    for i, c := range cols {
        esc[i] = strings.ReplaceAll(c, "\x00", "\x00\xff")
    }
    return strings.Join(esc, "\x00\x01")
}

row, err := table.ReadRow(ctx, cmvKey("region-a", "account-42", "9999999", "type-x", "item-abc", srcKey))
```

## What's Changed

### New Files
- `bttest/cmv.go` — CMV config types, registry, key transformation logic
- `bttest/sql_parse.go` — SQL parser for extracting CMV config from a `CreateMaterializedView` query
- `bttest/cmv_migrate.go` — One-time startup migration for views with legacy `#`-joined keys
- `bttest/cmv_test.go` — Tests for key transformation, write sync, delete propagation, key migration
- `bttest/sql_parse_test.go` — Tests for the SQL parser

### Modified Files
- `little_bigtable.go` — Version bump to 0.2.0
- `bttest/inmem.go` — Added `cmvs` field to server struct, CMV registration,
  shadow table creation, write-time sync hooks in MutateRow/MutateRows/
  CheckAndMutateRow/ReadModifyWriteRow/DropRowRange; runs the legacy key migration on startup
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
  retroactively copied. The only exception is the startup key migration below, which rebuilds
  a view that still has legacy keys from every row currently in the source table.
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
Becomes CMV key (shown with `\x00\x01` written out):
```
region-a\x00\x01account-42\x00\x019999999\x00\x01type-x\x00\x01item-abc\x00\x01item-abc#9999999#type-x#region-a#account-42
```

This matches prod Bigtable's encoding for multi-column view keys: string columns appear in
`ORDER BY` order, each terminated with `\x00\x01` (a `\x00` inside a value is escaped as
`\x00\xff`), and the final column is left unterminated. The source key's `#` layout is not
reused for the view key; the original source key is the last column (because `_key AS src_key`
appears in the `ORDER BY`).

## Startup Migration for Legacy Keys

View rows written by earlier versions of the emulator used `#`-joined keys
(for example `region-a#account-42#9999999#type-x#item-abc#<src_key>`). On startup, after
tables and views are loaded and before the server accepts requests, the emulator runs
`MigrateLegacyCMVKeys`:

1. For each registered view whose shadow table exists, it scans the view's row keys.
2. A key is legacy if the view has two or more key columns and the key has no `\x00\x01`.
   Every key written today has at least one `\x00\x01`, so the check is exact.
   Views with a single key column have the same keys in both formats and are skipped.
3. If a view has any legacy keys, the emulator rewrites it from the source table. It builds a
   row with the current encoding for every source row, then deletes the legacy rows. This also
   removes view rows whose source row no longer exists.
4. If the source table is missing, the view is left unchanged and the emulator logs a warning.

No version marker is stored. After a view is rebuilt it has no legacy keys, so the next
startup finds nothing to do. The migration is safe to interrupt: new rows are written before
old rows are deleted, so the next startup detects the remaining legacy rows and runs again.
Rows are read and written in batches of 1000, so memory use does not grow with table size.

The emulator logs `migrating CMV "<id>"` and `migrated CMV "<id>"` lines when it changes a view.
