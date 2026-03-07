import type { DeltaStore } from "./store/DeltaStore.js";
import type { AddAction, MetadataAction, ProtocolAction } from "./types/actions.js";
import type { StructType } from "./types/schema.js";
import { LogReplay } from "./log/LogReplay.js";
import type { DeltaSnapshot } from "./log/LogReplay.js";
import { FetchStore } from "./store/FetchStore.js";
import type { FileStatistics } from "./types/actions.js";
import { Result } from "@praha/byethrow";
import { DeltaError } from "./errors/index.js";
import type {
  TableNotFoundError,
  StoreError,
  LogNotFoundError,
  CheckpointReadError,
  InvalidLogEntryError,
  UnsupportedProtocolError,
  SchemaParseError,
} from "./errors/index.js";

export interface DeltaTableOptions {
  store?: DeltaStore;
  url?: string;
  fetchImpl?: typeof globalThis.fetch;
}

type OpenError =
  | TableNotFoundError
  | StoreError
  | LogNotFoundError
  | CheckpointReadError
  | InvalidLogEntryError
  | UnsupportedProtocolError
  | SchemaParseError;

type AtVersionError =
  | LogNotFoundError
  | CheckpointReadError
  | InvalidLogEntryError
  | UnsupportedProtocolError
  | SchemaParseError;

export type DeltaTable = {
  readonly version: () => number;
  readonly schema: () => StructType;
  readonly metadata: () => MetadataAction;
  readonly protocol: () => ProtocolAction;
  readonly files: () => AddAction[];
  readonly filePaths: () => string[];
  readonly partitionColumns: () => string[];
  readonly numRecords: () => number | null;
  readonly atVersion: (
    version: number,
  ) => Result.ResultAsync<DeltaTable, AtVersionError>;
};

function resolveStore(
  options: DeltaTableOptions,
): Result.Result<{ store: DeltaStore; tablePath: string }, TableNotFoundError> {
  if (options.store) {
    return Result.succeed({ store: options.store, tablePath: "" });
  }
  if (options.url) {
    const store = FetchStore.create({
      baseUrl: options.url,
      fetchImpl: options.fetchImpl,
    });
    return Result.succeed({ store, tablePath: "" });
  }
  return Result.fail(
    DeltaError.tableNotFound(
      "Either 'store' or 'url' must be provided in DeltaTableOptions",
    ),
  );
}

function createTable(
  store: DeltaStore,
  tablePath: string,
  snapshot: DeltaSnapshot,
): DeltaTable {
  return {
    version: () => snapshot.version,
    schema: () => snapshot.schema,
    metadata: () => snapshot.metadata,
    protocol: () => snapshot.protocol,
    files: () => snapshot.activeFiles,
    filePaths: () => snapshot.activeFiles.map((f) => f.path),
    partitionColumns: () => snapshot.metadata.partitionColumns,
    numRecords: () => {
      let total = 0;
      for (const file of snapshot.activeFiles) {
        if (file.stats) {
          try {
            const stats = JSON.parse(file.stats) as FileStatistics;
            total += stats.numRecords;
          } catch {
            return null;
          }
        } else {
          return null;
        }
      }
      return total;
    },
    atVersion: async (version) => {
      const replay = LogReplay.create(store, tablePath);
      const snapshotResult = await replay.buildSnapshot(version);
      if (Result.isFailure(snapshotResult)) return snapshotResult;
      return Result.succeed(createTable(store, tablePath, snapshotResult.value));
    },
  };
}

export const DeltaTable = {
  open: async (
    options: DeltaTableOptions,
  ): Result.ResultAsync<DeltaTable, OpenError> => {
    const resolved = resolveStore(options);
    if (Result.isFailure(resolved)) return resolved;

    const { store, tablePath } = resolved.value;
    const replay = LogReplay.create(store, tablePath);
    const snapshotResult = await replay.buildLatestSnapshot();
    if (Result.isFailure(snapshotResult)) return snapshotResult;

    return Result.succeed(createTable(store, tablePath, snapshotResult.value));
  },
};
