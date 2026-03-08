import type { DeltaStore } from "../../src/store/DeltaStore.js";
import { DeltaError } from "../../src/errors/DeltaError.js";
import { Result } from "@praha/byethrow";

export type MockStore = DeltaStore & {
  readonly addFile: (path: string, content: string) => void;
};

export const MockStore = {
  create: (): MockStore => {
    const files = new Map<string, string>();

    return {
      addFile: (path, content) => {
        files.set(path, content);
      },

      read: async (path) => {
        const content = files.get(path);
        if (content === undefined) {
          return Result.fail(DeltaError.storeError(`File not found: ${path}`));
        }
        return Result.succeed(content);
      },

      readBytes: async (path, start?, end?) => {
        const content = files.get(path);
        if (content === undefined) {
          return Result.fail(DeltaError.storeError(`File not found: ${path}`));
        }
        const encoder = new TextEncoder();
        const bytes = encoder.encode(content);
        if (start !== undefined || end !== undefined) {
          return Result.succeed(bytes.slice(start ?? 0, end).buffer);
        }
        return Result.succeed(bytes.buffer);
      },

      list: async (directory) => {
        const prefix = directory.endsWith("/") ? directory : `${directory}/`;
        const results: string[] = [];
        for (const key of files.keys()) {
          if (key.startsWith(prefix)) {
            const rest = key.slice(prefix.length);
            if (!rest.includes("/")) {
              results.push(rest);
            }
          }
        }
        return Result.succeed(results.sort());
      },

      exists: async (path) => {
        return files.has(path);
      },

      fileSize: async (path) => {
        const content = files.get(path);
        if (content === undefined) {
          return Result.fail(DeltaError.storeError(`File not found: ${path}`));
        }
        return Result.succeed(new TextEncoder().encode(content).length);
      },
    };
  },
};
