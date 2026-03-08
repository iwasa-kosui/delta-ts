import { describe, it, expect } from "vitest";
import { LogReader } from "../../src/log/LogReader.js";
import { MockStore } from "../helpers/MockStore.js";
import { Result } from "@praha/byethrow";

function createStoreWithLog(): MockStore {
  const store = MockStore.create();

  store.addFile(
    "_delta_log/00000000000000000000.json",
    [
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      '{"metaData":{"id":"test","format":{"provider":"parquet"},"schemaString":"{}","partitionColumns":[],"configuration":{}}}',
      '{"add":{"path":"file1.parquet","partitionValues":{},"size":100,"modificationTime":1000,"dataChange":true}}',
    ].join("\n"),
  );

  store.addFile(
    "_delta_log/00000000000000000001.json",
    [
      '{"add":{"path":"file2.parquet","partitionValues":{},"size":200,"modificationTime":2000,"dataChange":true}}',
      '{"remove":{"path":"file1.parquet","deletionTimestamp":2000,"dataChange":true}}',
    ].join("\n"),
  );

  return store;
}

describe("LogReader", () => {
  it("should read a specific version", async () => {
    const store = createStoreWithLog();
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    expect(actions).toHaveLength(3);
    expect(actions[0].type).toBe("protocol");
    expect(actions[1].type).toBe("metaData");
    expect(actions[2].type).toBe("add");
  });

  it("should return VERSION_NOT_FOUND for missing version", async () => {
    const store = createStoreWithLog();
    const reader = LogReader.create(store, "");

    const result = await reader.readVersion(99);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.type).toBe("VERSION_NOT_FOUND");
    }
  });

  it("should list available versions", async () => {
    const store = createStoreWithLog();
    const reader = LogReader.create(store, "");

    const versions = Result.unwrap(await reader.listVersions());
    expect(versions).toEqual([0, 1]);
  });

  it("should return latest version", async () => {
    const store = createStoreWithLog();
    const reader = LogReader.create(store, "");

    const latest = Result.unwrap(await reader.latestVersion());
    expect(latest).toBe(1);
  });

  it("should return null when no _last_checkpoint exists", async () => {
    const store = createStoreWithLog();
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).toBeNull();
  });

  it("should read _last_checkpoint when present", async () => {
    const store = createStoreWithLog();
    store.addFile(
      "_delta_log/_last_checkpoint",
      '{"version":10,"size":100}',
    );
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).toEqual({ version: 10, size: 100 });
  });
});
