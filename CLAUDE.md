# CLAUDE.md

## Project Overview

`delta-ts` — A lightweight TypeScript reader for Delta Lake tables. Read-only, zero-dependency (aside from `hyparquet` for Parquet and `@praha/byethrow` for Result types).

## Language

- All code, comments, commit messages, PR titles, and documentation MUST be in **English**.
- Japanese is only used inside `<details>` blocks in PR descriptions for Japanese-speaking readers.

## Commands

```bash
pnpm run type-check  # TypeScript type checking (tsc --noEmit)
pnpm run test        # Unit tests + delta-rs compatibility tests (vitest)
pnpm run build       # Build with tsup
```

## Architecture

### Companion Object Pattern

Types and their constructors/factories share the same name via TypeScript's declaration merging:

```ts
export type FetchStore = DeltaStore;
export const FetchStore = { create: (options) => { ... } };
```

This applies to `DeltaTable`, `FetchStore`, `LogReader`, `CheckpointReader`, `LogReplay`, and `DeltaAction`.

### Result Type — `@praha/byethrow`

All fallible operations return `Result.Result<T, E>` or `Result.ResultAsync<T, E>` (which is `Promise<Result<T, E>>`).

Key API mappings:

| Operation | Code |
|---|---|
| Create success | `Result.succeed(value)` |
| Create failure | `Result.fail(error)` |
| Check success | `Result.isSuccess(result)` |
| Check failure | `Result.isFailure(result)` |
| Unwrap (throws) | `Result.unwrap(result)` |
| Transform value | `Result.pipe(result, Result.map(fn))` |
| Chain fallible | `Result.pipe(result, Result.andThen(fn))` |
| Transform error | `Result.pipe(result, Result.mapError(fn))` |

Discriminants: `result.type === "Success"` / `result.type === "Failure"`.

### Railway Oriented Programming (ROP)

Prefer `Result.pipe` chains over imperative if/return-early when composing pure transformations. Extract small named functions as pipeline steps:

```ts
// Good — declarative pipeline
return Result.pipe(filesResult, Result.map(extractVersions), Result.andThen(getLatest));

// Acceptable — imperative early-return for stateful loops
if (Result.isFailure(r)) return r;
```

### Error Types

Errors are plain objects with a `type` discriminant (e.g., `"STORE_ERROR"`, `"VERSION_NOT_FOUND"`). Defined in `src/errors/DeltaError.ts` with factory functions on the `DeltaError` companion object.

### Project Structure

```
src/
  DeltaTable.ts        — Entry point: open / atVersion
  index.ts             — Public API re-exports
  store/
    DeltaStore.ts      — Store interface (read, readBytes, list, exists, fileSize)
    FetchStore.ts      — HTTP-based store implementation
  log/
    LogReader.ts       — Read individual log versions, list/latest version
    CheckpointReader.ts — Read Parquet checkpoint files
    LogReplay.ts       — Replay log to build DeltaSnapshot
  types/
    actions.ts         — Delta action types (add, remove, metaData, protocol, etc.)
    schema.ts          — Schema parsing (StructType, ArrayType, MapType, etc.)
  errors/
    DeltaError.ts      — Error type definitions and factories
  utils/
    path.ts            — Path helpers (version ↔ filename, deltaLogPath)
test/
  helpers/
    MockStore.ts       — In-memory store for unit tests
    FileSystemStore.ts — File-system store for integration tests
  delta-rs-compat/     — Tests verifying compatibility with delta-rs reference tables
```

## Conventions

- Commits follow **Conventional Commits**: `<type>(<scope>): <description>`
- PRs are created as **drafts** with `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>`
- Never amend + force push — always create new commits
- `dependencies` vs `devDependencies`: runtime-used packages (byethrow, hyparquet) go in `dependencies`
