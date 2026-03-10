import { describe, it, expect } from "vitest";

describe.skip("DenoStore", () => {
  it("requires Deno runtime — skipped in Node/Vitest", () => {
    expect(true).toBe(true);
  });
});
