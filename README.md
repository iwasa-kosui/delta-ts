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

All operations return `Result<T, E>` from `@praha/byethrow`. Errors are plain objects with a `type` discriminant, enabling exhaustive pattern matching.

#### Basic Error Checking

```ts
const result = await DeltaTable.open({ url: "https://..." });

if (Result.isFailure(result)) {
  console.error(result.error.type, result.error.message);
  return;
}

// result.value is narrowed to DeltaTable
const table = result.value;
```

#### Exhaustive Error Handling

`DeltaTable.open` can produce the following error types. Use a `switch` statement with a `default: never` check to ensure all cases are handled at compile time:

```ts
import { DeltaTable, DeltaError, Result } from "delta-ts";

const result = await DeltaTable.open({ url: "https://..." });

if (Result.isFailure(result)) {
  const error = result.error;

  switch (error.type) {
    case "TABLE_NOT_FOUND":
      console.error("Table not found:", error.message);
      break;
    case "STORE_ERROR":
      console.error("Storage read failed:", error.message);
      break;
    case "LOG_NOT_FOUND":
      console.error("Transaction log missing:", error.message);
      break;
    case "INVALID_LOG_ENTRY":
      console.error(`Invalid log at version ${error.version}:`, error.message);
      break;
    case "CHECKPOINT_READ_ERROR":
      console.error(`Checkpoint error at version ${error.version}:`, error.message);
      break;
    case "UNSUPPORTED_PROTOCOL":
      console.error(
        `Reader version ${error.requiredVersion} required, but only ${error.supportedVersion} is supported`,
      );
      break;
    case "SCHEMA_PARSE_ERROR":
      console.error("Failed to parse schema:", error.message);
      break;
    default: {
      // Compile-time exhaustiveness check — if a new error type is added,
      // TypeScript will report an error here until you handle it.
      const _exhaustive: never = error;
      throw new Error(`Unhandled error type: ${(_exhaustive as DeltaError).type}`);
    }
  }
}
```

#### Time-Travel Errors

`atVersion()` returns a subset of errors (no `TABLE_NOT_FOUND` or `STORE_ERROR`):

```ts
const v0 = await table.atVersion(0);

if (Result.isFailure(v0)) {
  switch (v0.error.type) {
    case "LOG_NOT_FOUND":
    case "INVALID_LOG_ENTRY":
    case "CHECKPOINT_READ_ERROR":
    case "UNSUPPORTED_PROTOCOL":
    case "SCHEMA_PARSE_ERROR":
      console.error(v0.error.message);
      break;
    default: {
      const _exhaustive: never = v0.error;
      throw new Error(`Unhandled error type: ${(_exhaustive as DeltaError).type}`);
    }
  }
}
```

#### Error Type Reference

| Error Type | Extra Fields | When |
|---|---|---|
| `TABLE_NOT_FOUND` | — | No `store` or `url` provided |
| `STORE_ERROR` | — | Storage backend read failure |
| `LOG_NOT_FOUND` | — | Transaction log directory missing |
| `INVALID_LOG_ENTRY` | `version`, `cause?` | Malformed JSON log entry |
| `CHECKPOINT_READ_ERROR` | `version`, `cause?` | Failed to read Parquet checkpoint |
| `UNSUPPORTED_PROTOCOL` | `requiredVersion`, `supportedVersion` | Table requires unsupported reader version |
| `SCHEMA_PARSE_ERROR` | `cause?` | Invalid schema in metadata |

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

## Acknowledgments

This project is deeply inspired by [delta-rs](https://github.com/delta-io/delta-rs), the Rust implementation of the Delta Lake protocol. The delta-rs codebase served as an invaluable reference for understanding the Delta Lake transaction log protocol, and many of our compatibility tests are ported directly from its test suite. We are grateful to the delta-rs maintainers and contributors for their excellent work.

Built on the [Delta Lake](https://delta.io/) open table format.

## License

MIT
