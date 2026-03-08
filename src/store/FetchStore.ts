import type { DeltaStore } from "./DeltaStore.js";
import { DeltaError } from "../errors/index.js";
import { Result } from "@praha/byethrow";

export interface FetchStoreOptions {
  baseUrl: string;
  fetchImpl?: typeof globalThis.fetch;
}

export type FetchStore = DeltaStore;

export const FetchStore = {
  create: (options: FetchStoreOptions): FetchStore => {
    const baseUrl = options.baseUrl.replace(/\/$/, "");
    const fetchFn = options.fetchImpl ?? globalThis.fetch.bind(globalThis);

    function url(path: string): string {
      return `${baseUrl}/${path.replace(/^\//, "")}`;
    }

    return {
      read: async (path) => {
        const response = await fetchFn(url(path));
        if (!response.ok) {
          return Result.fail(
            DeltaError.storeError(
              `Failed to read ${path}: ${response.status} ${response.statusText}`,
            ),
          );
        }
        return Result.succeed(await response.text());
      },

      readBytes: async (path, start?, end?) => {
        const headers: Record<string, string> = {};
        if (start !== undefined || end !== undefined) {
          const rangeStart = start ?? 0;
          const rangeEnd = end !== undefined ? end - 1 : "";
          headers["Range"] = `bytes=${rangeStart}-${rangeEnd}`;
        }
        const response = await fetchFn(url(path), { headers });
        if (!response.ok && response.status !== 206) {
          return Result.fail(
            DeltaError.storeError(
              `Failed to read bytes from ${path}: ${response.status} ${response.statusText}`,
            ),
          );
        }
        return Result.succeed(await response.arrayBuffer());
      },

      list: async (directory) => {
        const response = await fetchFn(url(directory));
        if (!response.ok) {
          return Result.fail(
            DeltaError.storeError(
              `Failed to list ${directory}: ${response.status} ${response.statusText}`,
            ),
          );
        }
        const text = await response.text();
        return Result.succeed(
          text
            .split("\n")
            .map((line) => line.trim())
            .filter((line) => line.length > 0),
        );
      },

      exists: async (path) => {
        const response = await fetchFn(url(path), { method: "HEAD" });
        return response.ok;
      },

      fileSize: async (path) => {
        const response = await fetchFn(url(path), { method: "HEAD" });
        if (!response.ok) {
          return Result.fail(
            DeltaError.storeError(
              `Failed to get file size for ${path}: ${response.status} ${response.statusText}`,
            ),
          );
        }
        const contentLength = response.headers.get("content-length");
        if (contentLength === null) {
          return Result.fail(
            DeltaError.storeError(`No content-length header for ${path}`),
          );
        }
        return Result.succeed(parseInt(contentLength, 10));
      },
    };
  },
};
