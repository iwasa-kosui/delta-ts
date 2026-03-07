import { describe, it, expect, vi } from "vitest";
import { FetchStore } from "../../src/store/FetchStore.js";
import { Result } from "@praha/byethrow";

function mockFetch(responses: Record<string, { status: number; body?: string; headers?: Record<string, string> }>) {
  return vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    const urlStr = typeof url === "string" ? url : url.toString();
    const resp = responses[urlStr];
    if (!resp) {
      return new Response("Not Found", { status: 404 });
    }
    return new Response(resp.body ?? "", {
      status: resp.status,
      headers: resp.headers,
    });
  }) as unknown as typeof globalThis.fetch;
}

describe("FetchStore", () => {
  it("should read text content", async () => {
    const fetch = mockFetch({
      "https://example.com/data/file.json": {
        status: 200,
        body: '{"key":"value"}',
      },
    });
    const store = FetchStore.create({ baseUrl: "https://example.com/data", fetchImpl: fetch });

    const result = Result.unwrap(await store.read("file.json"));
    expect(result).toBe('{"key":"value"}');
  });

  it("should return error on failed read", async () => {
    const fetch = mockFetch({});
    const store = FetchStore.create({ baseUrl: "https://example.com/data", fetchImpl: fetch });

    const result = await store.read("missing.json");
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.type).toBe("STORE_ERROR");
    }
  });

  it("should check existence with HEAD request", async () => {
    const fetch = mockFetch({
      "https://example.com/data/exists.json": { status: 200 },
    });
    const store = FetchStore.create({ baseUrl: "https://example.com/data", fetchImpl: fetch });

    expect(await store.exists("exists.json")).toBe(true);
    expect(await store.exists("missing.json")).toBe(false);
  });

  it("should get file size from content-length", async () => {
    const fetch = mockFetch({
      "https://example.com/data/file.parquet": {
        status: 200,
        headers: { "content-length": "4096" },
      },
    });
    const store = FetchStore.create({ baseUrl: "https://example.com/data", fetchImpl: fetch });

    const size = Result.unwrap(await store.fileSize("file.parquet"));
    expect(size).toBe(4096);
  });

  it("should strip trailing slash from base URL", async () => {
    const fetch = mockFetch({
      "https://example.com/data/file.json": { status: 200, body: "ok" },
    });
    const store = FetchStore.create({ baseUrl: "https://example.com/data/", fetchImpl: fetch });

    const result = Result.unwrap(await store.read("file.json"));
    expect(result).toBe("ok");
  });

  it("should list directory contents", async () => {
    const fetch = mockFetch({
      "https://example.com/data/dir": {
        status: 200,
        body: "file1.json\nfile2.json\nfile3.json\n",
      },
    });
    const store = FetchStore.create({ baseUrl: "https://example.com/data", fetchImpl: fetch });

    const files = Result.unwrap(await store.list("dir"));
    expect(files).toEqual(["file1.json", "file2.json", "file3.json"]);
  });
});
