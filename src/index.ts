export { DeltaTable } from "./DeltaTable.js";
export type { DeltaTableOptions } from "./DeltaTable.js";

export type {
  AddAction,
  RemoveAction,
  MetadataAction,
  ProtocolAction,
  CommitInfoAction,
  TxnAction,
  DomainMetadataAction,
  DeletionVector,
  FileStatistics,
  LastCheckpointInfo,
  DeltaDataType,
  StructType,
  StructField,
  ArrayType,
  MapType,
  PrimitiveType,
} from "./types/index.js";

export { DeltaAction, parseSchema } from "./types/index.js";

export type { DeltaStore } from "./store/index.js";
export { FetchStore } from "./store/index.js";
export type { FetchStoreOptions } from "./store/index.js";

export type { DeltaSnapshot } from "./log/index.js";

export { DeltaError } from "./errors/index.js";
export type {
  DeltaErrorType,
  TableNotFoundError,
  LogNotFoundError,
  InvalidLogEntryError,
  CheckpointReadError,
  UnsupportedProtocolError,
  SchemaParseError,
  StoreError,
  VersionNotFoundError,
} from "./errors/index.js";

export { Result } from "@praha/byethrow";
