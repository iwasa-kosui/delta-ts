export type TableNotFoundError = {
  readonly type: "TABLE_NOT_FOUND";
  readonly message: string;
};

export type LogNotFoundError = {
  readonly type: "LOG_NOT_FOUND";
  readonly message: string;
};

export type InvalidLogEntryError = {
  readonly type: "INVALID_LOG_ENTRY";
  readonly message: string;
  readonly version: number;
  readonly cause?: unknown;
};

export type CheckpointReadError = {
  readonly type: "CHECKPOINT_READ_ERROR";
  readonly message: string;
  readonly version: number;
  readonly cause?: unknown;
};

export type UnsupportedProtocolError = {
  readonly type: "UNSUPPORTED_PROTOCOL";
  readonly message: string;
  readonly requiredVersion: number;
  readonly supportedVersion: number;
};

export type SchemaParseError = {
  readonly type: "SCHEMA_PARSE_ERROR";
  readonly message: string;
  readonly cause?: unknown;
};

export type StoreError = {
  readonly type: "STORE_ERROR";
  readonly message: string;
};

export type VersionNotFoundError = {
  readonly type: "VERSION_NOT_FOUND";
  readonly message: string;
  readonly version: number;
  readonly cause?: unknown;
};

export type DeltaError =
  | TableNotFoundError
  | LogNotFoundError
  | InvalidLogEntryError
  | CheckpointReadError
  | UnsupportedProtocolError
  | SchemaParseError
  | StoreError
  | VersionNotFoundError;

export type DeltaErrorType = DeltaError["type"];

const DELTA_ERROR_TYPES: ReadonlySet<string> = new Set<DeltaErrorType>([
  "TABLE_NOT_FOUND",
  "LOG_NOT_FOUND",
  "INVALID_LOG_ENTRY",
  "CHECKPOINT_READ_ERROR",
  "UNSUPPORTED_PROTOCOL",
  "SCHEMA_PARSE_ERROR",
  "STORE_ERROR",
  "VERSION_NOT_FOUND",
]);

export const DeltaError = {
  tableNotFound: (message: string): TableNotFoundError => ({
    type: "TABLE_NOT_FOUND",
    message,
  }),

  logNotFound: (message: string): LogNotFoundError => ({
    type: "LOG_NOT_FOUND",
    message,
  }),

  invalidLogEntry: (
    message: string,
    version: number,
    cause?: unknown,
  ): InvalidLogEntryError => ({
    type: "INVALID_LOG_ENTRY",
    message,
    version,
    cause,
  }),

  checkpointReadError: (
    message: string,
    version: number,
    cause?: unknown,
  ): CheckpointReadError => ({
    type: "CHECKPOINT_READ_ERROR",
    message,
    version,
    cause,
  }),

  unsupportedProtocol: (
    requiredVersion: number,
    supportedVersion: number,
  ): UnsupportedProtocolError => ({
    type: "UNSUPPORTED_PROTOCOL",
    message: `Table requires reader version ${requiredVersion}, but only version ${supportedVersion} is supported`,
    requiredVersion,
    supportedVersion,
  }),

  schemaParseError: (
    message: string,
    cause?: unknown,
  ): SchemaParseError => ({
    type: "SCHEMA_PARSE_ERROR",
    message,
    cause,
  }),

  storeError: (message: string): StoreError => ({
    type: "STORE_ERROR",
    message,
  }),

  versionNotFound: (
    version: number,
    cause?: unknown,
  ): VersionNotFoundError => ({
    type: "VERSION_NOT_FOUND",
    message: `Version ${version} not found`,
    version,
    cause,
  }),

  is: (value: unknown): value is DeltaError => {
    if (typeof value !== "object" || value === null) return false;
    const obj = value as Record<string, unknown>;
    return (
      typeof obj.type === "string" && DELTA_ERROR_TYPES.has(obj.type as string)
    );
  },
};
