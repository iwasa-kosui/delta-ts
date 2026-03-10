import { describe, it, expect } from "vitest";

describe.skip("BunStore", () => {
  it("requires Bun runtime — skipped in Node/Vitest", () => {
    expect(true).toBe(true);
  });
});
