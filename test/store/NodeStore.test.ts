import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, writeFile, rm, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "@praha/byethrow";
import { NodeStore } from "../../src/store-node/NodeStore.js";

describe("NodeStore", () => {
  let tempDir: string;
  let store: ReturnType<typeof NodeStore.create>;

  beforeAll(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "nodestore-test-"));
    store = NodeStore.create(tempDir);

    await writeFile(join(tempDir, "hello.txt"), "Hello, world!");
    await writeFile(join(tempDir, "binary.bin"), Buffer.from([0, 1, 2, 3, 4]));
    await mkdir(join(tempDir, "subdir"));
    await writeFile(join(tempDir, "subdir", "a.txt"), "a");
    await writeFile(join(tempDir, "subdir", "b.txt"), "b");
  });

  afterAll(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  describe("read", () => {
    it("reads a text file", async () => {
      const result = await store.read("hello.txt");
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(result.value).toBe("Hello, world!");
      }
    });

    it("returns failure for missing file", async () => {
      const result = await store.read("nonexistent.txt");
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.error.type).toBe("STORE_ERROR");
      }
    });
  });

  describe("readBytes", () => {
    it("reads full file as ArrayBuffer", async () => {
      const result = await store.readBytes("binary.bin");
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(new Uint8Array(result.value)).toEqual(
          new Uint8Array([0, 1, 2, 3, 4]),
        );
      }
    });

    it("reads a byte range", async () => {
      const result = await store.readBytes("binary.bin", 1, 4);
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(new Uint8Array(result.value)).toEqual(
          new Uint8Array([1, 2, 3]),
        );
      }
    });

    it("returns failure for missing file", async () => {
      const result = await store.readBytes("nonexistent.bin");
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.error.type).toBe("STORE_ERROR");
      }
    });
  });

  describe("list", () => {
    it("lists directory entries sorted", async () => {
      const result = await store.list("subdir");
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(result.value).toEqual(["a.txt", "b.txt"]);
      }
    });

    it("returns empty array for nonexistent directory", async () => {
      const result = await store.list("nonexistent");
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(result.value).toEqual([]);
      }
    });
  });

  describe("exists", () => {
    it("returns true for existing file", async () => {
      expect(await store.exists("hello.txt")).toBe(true);
    });

    it("returns false for nonexistent file", async () => {
      expect(await store.exists("nonexistent.txt")).toBe(false);
    });
  });

  describe("fileSize", () => {
    it("returns file size", async () => {
      const result = await store.fileSize("binary.bin");
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        expect(result.value).toBe(5);
      }
    });

    it("returns failure for missing file", async () => {
      const result = await store.fileSize("nonexistent.bin");
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.error.type).toBe("STORE_ERROR");
      }
    });
  });
});
