# delta-ts

Lightweight, read-only TypeScript library for [Delta Lake](https://delta.io/) tables.

- **Runtime-agnostic** — works in Node.js, Deno, Bun, browsers, and edge runtimes (no `fs` dependency)
- **Minimal dependencies** — only [hyparquet](https://github.com/hyparam/hyparquet) (Parquet reader) and [@praha/byethrow](https://github.com/praha-inc/byethrow) (Result type)
- **Type-safe error handling** — all fallible operations return `Result<T, E>` instead of throwing

## Install

```bash
npm install delta-ts
```

## Quick Start

```ts
import { DeltaTable, Result } from "delta-ts";

// Open a table from a URL
const result = await DeltaTable.open({
  url: "https://example.com/path/to/delta-table",
});

if (Result.isSuccess(result)) {
  const table = result.value;

  console.log("Version:", table.version());
  console.log("Schema:", table.schema());
  console.log("Files:", table.filePaths());
  console.log("Partitions:", table.partitionColumns());
  console.log("Records:", table.numRecords());
}
```

## Usage

### Opening a Table

```ts
// From a URL (uses fetch internally)
const result = await DeltaTable.open({ url: "https://..." });

// With a custom fetch implementation
const result = await DeltaTable.open({
  url: "https://...",
  fetchImpl: myCustomFetch,
});

// With a custom store
const result = await DeltaTable.open({ store: myStore });
```

### Time Travel

```ts
const table = Result.unwrap(await DeltaTable.open({ url: "https://..." }));

// Read a specific version
const v0Result = await table.atVersion(0);
if (Result.isSuccess(v0Result)) {
  console.log("Files at v0:", v0Result.value.filePaths());
}
```

### Error Handling

All operations return `Result<T, E>` from `@praha/byethrow`. Errors are plain objects with a `type` discriminant:

```ts
const result = await DeltaTable.open({ url: "https://..." });

if (Result.isFailure(result)) {
  switch (result.error.type) {
    case "TABLE_NOT_FOUND":
    case "STORE_ERROR":
    case "LOG_NOT_FOUND":
    case "UNSUPPORTED_PROTOCOL":
    case "SCHEMA_PARSE_ERROR":
      console.error(result.error.message);
      break;
  }
}
```

### Custom Store

Implement the `DeltaStore` interface to read from any storage backend (S3, GCS, local filesystem, etc.):

```ts
import type { DeltaStore } from "delta-ts";
import { Result } from "@praha/byethrow";

const myStore: DeltaStore = {
  read: async (path) => Result.succeed(/* file content as string */),
  readBytes: async (path, start?, end?) => Result.succeed(/* ArrayBuffer */),
  list: async (directory) => Result.succeed(/* string[] of filenames */),
  exists: async (path) => /* boolean */,
  fileSize: async (path) => Result.succeed(/* number */),
};

const result = await DeltaTable.open({ store: myStore });
```

## API

### `DeltaTable.open(options)`

Opens a Delta table and returns the latest snapshot.

- `options.url` — Base URL of the Delta table
- `options.store` — Custom `DeltaStore` implementation
- `options.fetchImpl` — Custom fetch function (defaults to `globalThis.fetch`)

Returns `ResultAsync<DeltaTable, OpenError>`

### `DeltaTable` Instance

| Method | Return Type | Description |
|---|---|---|
| `version()` | `number` | Current snapshot version |
| `schema()` | `StructType` | Table schema |
| `metadata()` | `MetadataAction` | Table metadata |
| `protocol()` | `ProtocolAction` | Protocol version info |
| `files()` | `AddAction[]` | Active file entries |
| `filePaths()` | `string[]` | Active file paths |
| `partitionColumns()` | `string[]` | Partition column names |
| `numRecords()` | `number \| null` | Total record count (null if stats unavailable) |
| `atVersion(v)` | `ResultAsync<DeltaTable, Error>` | Time-travel to version `v` |

## Supported Features

- JSON transaction log reading and replay
- Parquet checkpoint files
- Add/remove file reconciliation
- Deletion vectors (parsed, not applied)
- Schema parsing (struct, array, map, primitives)
- Protocol version gating (reader version 1)

## License

MIT
