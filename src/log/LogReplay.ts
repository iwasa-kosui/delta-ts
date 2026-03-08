import type {
  AddAction,
  MetadataAction,
  ProtocolAction,
  DeltaAction,
} from "../types/actions.js";
import { parseSchema } from "../types/schema.js";
import type { StructType } from "../types/schema.js";
import { DeltaError } from "../errors/index.js";
import type {
  StoreError,
  LogNotFoundError,
  CheckpointReadError,
  InvalidLogEntryError,
  UnsupportedProtocolError,
  SchemaParseError,
} from "../errors/index.js";
import { Result } from "@praha/byethrow";
import { LogReader } from "./LogReader.js";
import { CheckpointReader } from "./CheckpointReader.js";
import type { DeltaStore } from "../store/DeltaStore.js";

export interface DeltaSnapshot {
  version: number;
  metadata: MetadataAction;
  protocol: ProtocolAction;
  activeFiles: AddAction[];
  schema: StructType;
}

const MAX_SUPPORTED_READER_VERSION = 1;

type BuildSnapshotError =
  | LogNotFoundError
  | CheckpointReadError
  | InvalidLogEntryError
  | UnsupportedProtocolError
  | SchemaParseError;

export type LogReplay = {
  readonly buildLatestSnapshot: () => Result.ResultAsync<
    DeltaSnapshot,
    StoreError | BuildSnapshotError
  >;
  readonly buildSnapshot: (
    targetVersion: number,
  ) => Result.ResultAsync<DeltaSnapshot, BuildSnapshotError>;
};

function validateMetadata(
  metadata: MetadataAction | null,
): Result.Result<MetadataAction, LogNotFoundError> {
  if (!metadata) {
    return Result.fail(
      DeltaError.logNotFound("No metadata action found in log"),
    );
  }
  return Result.succeed(metadata);
}

function validateProtocol(
  protocol: ProtocolAction | null,
): Result.Result<ProtocolAction, LogNotFoundError | UnsupportedProtocolError> {
  if (!protocol) {
    return Result.fail(
      DeltaError.logNotFound("No protocol action found in log"),
    );
  }
  if (protocol.minReaderVersion > MAX_SUPPORTED_READER_VERSION) {
    return Result.fail(
      DeltaError.unsupportedProtocol(
        protocol.minReaderVersion,
        MAX_SUPPORTED_READER_VERSION,
      ),
    );
  }
  return Result.succeed(protocol);
}

function parseTableSchema(
  schemaString: string,
): Result.Result<StructType, SchemaParseError> {
  try {
    return Result.succeed(parseSchema(schemaString));
  } catch (e) {
    return Result.fail(
      DeltaError.schemaParseError("Failed to parse table schema", e),
    );
  }
}

export const LogReplay = {
  create: (store: DeltaStore, tablePath: string): LogReplay => {
    const logReader = LogReader.create(store, tablePath);
    const checkpointReader = CheckpointReader.create(store, tablePath);

    function fileKey(
      path: string,
      deletionVector?: { pathOrInlineDv: string },
    ): string {
      if (deletionVector) {
        return `${path}::${deletionVector.pathOrInlineDv}`;
      }
      return path;
    }

    function replayActions(
      actions: DeltaAction[],
      activeFiles: Map<string, AddAction>,
      onMetadata: (m: MetadataAction) => void,
      onProtocol: (p: ProtocolAction) => void,
    ): void {
      for (const action of actions) {
        switch (action.type) {
          case "metaData":
            onMetadata(action.metaData);
            break;
          case "protocol":
            onProtocol(action.protocol);
            break;
          case "add": {
            const key = fileKey(action.add.path, action.add.deletionVector);
            activeFiles.set(key, action.add);
            break;
          }
          case "remove": {
            const key = fileKey(
              action.remove.path,
              action.remove.deletionVector,
            );
            activeFiles.delete(key);
            break;
          }
          case "commitInfo":
          case "txn":
          case "domainMetadata":
            break;
        }
      }
    }

    const buildSnapshot = async (
      targetVersion: number,
    ): Result.ResultAsync<DeltaSnapshot, BuildSnapshotError> => {
      const checkpoint = await logReader.getLastCheckpoint();

      let startVersion = 0;
      let metadata: MetadataAction | null = null;
      let protocol: ProtocolAction | null = null;
      const activeFiles = new Map<string, AddAction>();

      if (checkpoint && checkpoint.version <= targetVersion) {
        const cpResult = await checkpointReader.readCheckpoint(
          checkpoint.version,
        );
        if (Result.isFailure(cpResult)) return cpResult;
        replayActions(
          cpResult.value,
          activeFiles,
          (m) => { metadata = m; },
          (p) => { protocol = p; },
        );
        startVersion = checkpoint.version + 1;
      }

      for (let v = startVersion; v <= targetVersion; v++) {
        const actionsResult = await logReader.readVersion(v);
        if (Result.isFailure(actionsResult)) {
          if (actionsResult.error.type === "VERSION_NOT_FOUND") {
            continue;
          }
          return Result.fail(actionsResult.error);
        }
        replayActions(
          actionsResult.value,
          activeFiles,
          (m) => { metadata = m; },
          (p) => { protocol = p; },
        );
      }

      const metadataResult = validateMetadata(metadata);
      if (Result.isFailure(metadataResult)) return metadataResult;

      const protocolResult = validateProtocol(protocol);
      if (Result.isFailure(protocolResult)) return protocolResult;

      const schemaResult = parseTableSchema(metadataResult.value.schemaString);
      if (Result.isFailure(schemaResult)) return schemaResult;

      return Result.succeed({
        version: targetVersion,
        metadata: metadataResult.value,
        protocol: protocolResult.value,
        activeFiles: Array.from(activeFiles.values()),
        schema: schemaResult.value,
      });
    };

    return {
      buildLatestSnapshot: async () => {
        const latestResult = await logReader.latestVersion();
        if (Result.isFailure(latestResult)) return latestResult;
        return buildSnapshot(latestResult.value);
      },
      buildSnapshot,
    };
  },
};
