import type { DeltaStore } from "../store/DeltaStore.js";
import { DeltaAction } from "../types/actions.js";
import type { LastCheckpointInfo } from "../types/actions.js";
import { DeltaError } from "../errors/index.js";
import type {
  StoreError,
  LogNotFoundError,
  VersionNotFoundError,
  InvalidLogEntryError,
} from "../errors/index.js";
import { Result } from "@praha/byethrow";
import {
  versionToFilename,
  filenameToVersion,
  deltaLogPath,
} from "../utils/path.js";

export type LogReader = {
  readonly getLastCheckpoint: () => Promise<LastCheckpointInfo | null>;
  readonly readVersion: (
    version: number,
  ) => Result.ResultAsync<DeltaAction[], VersionNotFoundError | InvalidLogEntryError>;
  readonly listVersions: () => Result.ResultAsync<number[], StoreError>;
  readonly latestVersion: () => Result.ResultAsync<
    number,
    StoreError | LogNotFoundError
  >;
};

function parseLogContent(content: string): DeltaAction[] {
  const lines = content
    .split("\n")
    .filter((line) => line.trim().length > 0);
  const actions: DeltaAction[] = [];
  for (const line of lines) {
    const raw = JSON.parse(line) as Record<string, unknown>;
    const action = DeltaAction.parse(raw);
    if (action) actions.push(action);
  }
  return actions;
}

function extractVersions(files: string[]): number[] {
  const versions: number[] = [];
  for (const file of files) {
    const basename = file.split("/").pop() ?? file;
    const version = filenameToVersion(basename);
    if (version !== null) {
      versions.push(version);
    }
  }
  return versions.sort((a, b) => a - b);
}

function getLatest(versions: number[]): Result.Result<number, LogNotFoundError> {
  if (versions.length === 0) {
    return Result.fail(DeltaError.logNotFound("No log versions found"));
  }
  return Result.succeed(versions[versions.length - 1]);
}

export const LogReader = {
  create: (store: DeltaStore, tablePath: string): LogReader => {
    const logPath = deltaLogPath(tablePath);

    return {
      getLastCheckpoint: async () => {
        const checkpointPath = `${logPath}/_last_checkpoint`;
        const result = await store.read(checkpointPath);
        if (Result.isFailure(result)) return null;
        try {
          return JSON.parse(result.value) as LastCheckpointInfo;
        } catch {
          return null;
        }
      },

      readVersion: async (version) => {
        const filename = versionToFilename(version);
        const path = `${logPath}/${filename}`;

        const result = await store.read(path);
        return Result.pipe(
          result,
          Result.mapError((e) => DeltaError.versionNotFound(version, e)),
          Result.andThen((content) => {
            try {
              return Result.succeed(parseLogContent(content));
            } catch (e) {
              return Result.fail(
                DeltaError.invalidLogEntry(
                  `Failed to parse log entry for version ${version}`,
                  version,
                  e,
                ),
              );
            }
          }),
        );
      },

      listVersions: async () => {
        const filesResult = await store.list(logPath);
        return Result.pipe(filesResult, Result.map(extractVersions));
      },

      latestVersion: async () => {
        const filesResult = await store.list(logPath);
        return Result.pipe(
          filesResult,
          Result.map(extractVersions),
          Result.andThen(getLatest),
        );
      },
    };
  },
};
