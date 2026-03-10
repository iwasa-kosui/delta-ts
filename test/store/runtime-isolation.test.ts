import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const srcRoot = resolve(__dirname, "../../src");

function readSource(relativePath: string): string {
  return readFileSync(resolve(srcRoot, relativePath), "utf-8");
}

describe("Runtime isolation", () => {
  describe("NodeStore", () => {
    it("does not reference Bun APIs", () => {
      const source = readSource("store-node/NodeStore.ts");
      expect(source).not.toMatch(/\bBun\b/);
    });

    it("does not reference Deno APIs", () => {
      const source = readSource("store-node/NodeStore.ts");
      expect(source).not.toMatch(/\bDeno\b/);
    });
  });

  describe("BunStore", () => {
    it("does not reference Deno APIs", () => {
      const source = readSource("store-bun/BunStore.ts");
      expect(source).not.toMatch(/\bDeno\b/);
    });
  });

  describe("DenoStore", () => {
    it("does not import from node: modules", () => {
      const source = readSource("store-deno/DenoStore.ts");
      expect(source).not.toMatch(/from\s+["']node:/);
    });

    it("does not reference Bun APIs", () => {
      const source = readSource("store-deno/DenoStore.ts");
      expect(source).not.toMatch(/\bBun\b/);
    });
  });

  describe("Main index", () => {
    it("does not import store-node", () => {
      const source = readSource("index.ts");
      expect(source).not.toMatch(/store-node/);
    });

    it("does not import store-bun", () => {
      const source = readSource("index.ts");
      expect(source).not.toMatch(/store-bun/);
    });

    it("does not import store-deno", () => {
      const source = readSource("index.ts");
      expect(source).not.toMatch(/store-deno/);
    });
  });
});
