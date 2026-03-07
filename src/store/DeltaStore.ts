import type { Result } from "@praha/byethrow";
import type { StoreError } from "../errors/DeltaError.js";

export type DeltaStore = {
  readonly read: (path: string) => Result.ResultAsync<string, StoreError>;
  readonly readBytes: (
    path: string,
    start?: number,
    end?: number,
  ) => Result.ResultAsync<ArrayBuffer, StoreError>;
  readonly list: (directory: string) => Result.ResultAsync<string[], StoreError>;
  readonly exists: (path: string) => Promise<boolean>;
  readonly fileSize: (path: string) => Result.ResultAsync<number, StoreError>;
};
