import * as fs from "node:fs";
import * as path from "node:path";
import type { DeltaStore } from "../../src/store/DeltaStore.js";
import { DeltaError } from "../../src/errors/DeltaError.js";
import { Result } from "@praha/byethrow";

export type FileSystemStore = DeltaStore;

export const FileSystemStore = {
  create: (basePath: string): FileSystemStore => {
    function resolve(filePath: string): string {
      return path.join(basePath, filePath);
    }

    return {
      read: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          return Result.succeed(fs.readFileSync(fullPath, "utf-8"));
        } catch {
          return Result.fail(
            DeltaError.storeError(`File not found: ${fullPath}`),
          );
        }
      },

      readBytes: async (filePath, start?, end?) => {
        const fullPath = resolve(filePath);
        try {
          const buffer = fs.readFileSync(fullPath);
          const slice = buffer.slice(start ?? 0, end ?? buffer.length);
          return Result.succeed(
            slice.buffer.slice(
              slice.byteOffset,
              slice.byteOffset + slice.byteLength,
            ),
          );
        } catch {
          return Result.fail(
            DeltaError.storeError(`File not found: ${fullPath}`),
          );
        }
      },

      list: async (directory) => {
        const fullPath = resolve(directory);
        try {
          const entries = fs.readdirSync(fullPath);
          return Result.succeed(entries.sort());
        } catch {
          return Result.succeed([]);
        }
      },

      exists: async (filePath) => {
        const fullPath = resolve(filePath);
        return fs.existsSync(fullPath);
      },

      fileSize: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          const stat = fs.statSync(fullPath);
          return Result.succeed(stat.size);
        } catch {
          return Result.fail(
            DeltaError.storeError(`File not found: ${fullPath}`),
          );
        }
      },
    };
  },
};
