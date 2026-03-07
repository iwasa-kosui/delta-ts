import type { DeltaStore } from "../store/DeltaStore.js";
import { DeltaAction } from "../types/actions.js";
import { DeltaError } from "../errors/index.js";
import type { CheckpointReadError } from "../errors/index.js";
import { Result } from "@praha/byethrow";
import { versionToCheckpointFilename, deltaLogPath } from "../utils/path.js";

interface AsyncBuffer {
  byteLength: number;
  slice(start: number, end?: number): Promise<ArrayBuffer>;
}

export type CheckpointReader = {
  readonly readCheckpoint: (
    version: number,
  ) => Result.ResultAsync<DeltaAction[], CheckpointReadError>;
};

function createAsyncBuffer(
  store: DeltaStore,
  path: string,
  byteLength: number,
): AsyncBuffer {
  return {
    byteLength,
    slice: async (start: number, end?: number) => {
      const result = await store.readBytes(path, start, end);
      if (Result.isFailure(result)) {
        throw result.error;
      }
      return result.value;
    },
  };
}

function parseActions(rows: Record<string, unknown>[]): DeltaAction[] {
  const actions: DeltaAction[] = [];
  for (const row of rows) {
    const action = DeltaAction.parse(row);
    if (action) actions.push(action);
  }
  return actions;
}

export const CheckpointReader = {
  create: (store: DeltaStore, tablePath: string): CheckpointReader => {
    const logPath = deltaLogPath(tablePath);

    return {
      readCheckpoint: async (version) => {
        const filename = versionToCheckpointFilename(version);
        const path = `${logPath}/${filename}`;

        try {
          const { parquetRead } = await import("hyparquet");

          const fileSizeResult = await store.fileSize(path);
          if (Result.isFailure(fileSizeResult)) {
            return Result.fail(
              DeltaError.checkpointReadError(
                `Failed to read checkpoint at version ${version}`,
                version,
                fileSizeResult.error,
              ),
            );
          }

          const asyncBuffer = createAsyncBuffer(
            store,
            path,
            fileSizeResult.value,
          );

          const rows: Record<string, unknown>[] = [];
          await parquetRead({
            file: asyncBuffer,
            rowFormat: "object",
            onComplete: (data: Record<string, unknown>[]) => {
              rows.push(...data);
            },
          });

          return Result.succeed(parseActions(rows));
        } catch (e) {
          return Result.fail(
            DeltaError.checkpointReadError(
              `Failed to read checkpoint at version ${version}`,
              version,
              e,
            ),
          );
        }
      },
    };
  },
};
