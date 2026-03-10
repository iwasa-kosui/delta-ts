import type { DeltaStore } from "../store/DeltaStore.js";
import { DeltaError } from "../errors/DeltaError.js";
import { Result } from "@praha/byethrow";

function joinPath(...parts: string[]): string {
  return parts
    .map((p, i) => (i === 0 ? p.replace(/\/$/, "") : p.replace(/^\/|\/$/g, "")))
    .filter((p) => p.length > 0)
    .join("/");
}

export type DenoStore = DeltaStore;

export const DenoStore = {
  create: (basePath: string): DenoStore => {
    function resolve(filePath: string): string {
      return joinPath(basePath, filePath);
    }

    return {
      read: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          return Result.succeed(await Deno.readTextFile(fullPath));
        } catch {
          return Result.fail(
            DeltaError.storeError(`Failed to read file: ${fullPath}`),
          );
        }
      },

      readBytes: async (filePath, start?, end?) => {
        const fullPath = resolve(filePath);
        try {
          const data = await Deno.readFile(fullPath);
          const slice = data.subarray(start ?? 0, end ?? data.length);
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
          const entries: string[] = [];
          for await (const entry of Deno.readDir(fullPath)) {
            entries.push(entry.name);
          }
          return Result.succeed(entries.sort());
        } catch {
          return Result.succeed([]);
        }
      },

      exists: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          await Deno.stat(fullPath);
          return true;
        } catch {
          return false;
        }
      },

      fileSize: async (filePath) => {
        const fullPath = resolve(filePath);
        try {
          const s = await Deno.stat(fullPath);
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
