import { readFile, readdir, stat, access } from "node:fs/promises";
import { join } from "node:path";
import type { DeltaStore } from "../store/DeltaStore.js";
import { DeltaError } from "../errors/DeltaError.js";
import { Result } from "@praha/byethrow";

export type NodeStore = DeltaStore;

export const NodeStore = {
  create: (basePath: string): NodeStore => {
    function resolve(filePath: string): string {
      return join(basePath, filePath);
    }

    return {
      read: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          return Result.succeed(await readFile(fullPath, "utf-8"));
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to read file: ${fullPath}`),
          );
        }
      },

      readBytes: async (filePath, start?, end?) => {
        const fullPath = resolve(filePath);
        try {
          const buffer = await readFile(fullPath);
          const slice = buffer.subarray(start ?? 0, end ?? buffer.length);
          return Result.succeed(
            slice.buffer.slice(
              slice.byteOffset,
              slice.byteOffset + slice.byteLength,
            ),
          );
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
        try {
          await access(fullPath);
          return true;
        } catch {
          return false;
        }
      },

      fileSize: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          const s = await stat(fullPath);
          return Result.succeed(s.size);
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to get file size: ${fullPath}`),
          );
        }
      },
    };
  },
};
