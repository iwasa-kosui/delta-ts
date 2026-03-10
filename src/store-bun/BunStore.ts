import { readdir } from "node:fs/promises";
import { join } from "node:path";
import type { DeltaStore } from "../store/DeltaStore.js";
import { DeltaError } from "../errors/DeltaError.js";
import { Result } from "@praha/byethrow";

export type BunStore = DeltaStore;

export const BunStore = {
  create: (basePath: string): BunStore => {
    function resolve(filePath: string): string {
      return join(basePath, filePath);
    }

    return {
      read: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          return Result.succeed(await Bun.file(fullPath).text());
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to read file: ${fullPath}`),
          );
        }
      },

      readBytes: async (filePath, start?, end?) => {
        const fullPath = resolve(filePath);
        try {
          const buf = await Bun.file(fullPath).arrayBuffer();
          if (start !== undefined || end !== undefined) {
            return Result.succeed(buf.slice(start ?? 0, end ?? buf.byteLength));
          }
          return Result.succeed(buf);
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to read bytes from: ${fullPath}`),
          );
        }
      },

      list: async (directory) => {
        const fullPath = resolve(directory);
        try {
          const entries = await readdir(fullPath);
          return Result.succeed(entries.sort());
        } catch {
          return Result.succeed([]);
        }
      },

      exists: async (filePath) => {
        const fullPath = resolve(filePath);
        return Bun.file(fullPath).exists();
      },

      fileSize: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          const size = Bun.file(fullPath).size;
          return Result.succeed(size);
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to get file size: ${fullPath}`),
          );
        }
      },
    };
  },
};
