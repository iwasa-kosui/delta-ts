import { describe, it, expect } from "vitest";
import { LogReplay } from "../../src/log/LogReplay.js";
import { MockStore } from "../helpers/MockStore.js";
import { Result } from "@praha/byethrow";

const SCHEMA_STRING = JSON.stringify({
  type: "struct",
  fields: [
    { name: "id", type: "long", nullable: false, metadata: {} },
    { name: "name", type: "string", nullable: true, metadata: {} },
  ],
});

function createStoreWithLog(): MockStore {
  const store = MockStore.create();

  store.addFile(
    "_delta_log/00000000000000000000.json",
    [
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      `{"metaData":{"id":"test","format":{"provider":"parquet"},"schemaString":${JSON.stringify(SCHEMA_STRING)},"partitionColumns":[],"configuration":{}}}`,
      '{"add":{"path":"file1.parquet","partitionValues":{},"size":100,"modificationTime":1000,"dataChange":true}}',
      '{"add":{"path":"file2.parquet","partitionValues":{},"size":200,"modificationTime":1000,"dataChange":true}}',
    ].join("\n"),
  );

  store.addFile(
    "_delta_log/00000000000000000001.json",
    [
      '{"add":{"path":"file3.parquet","partitionValues":{},"size":300,"modificationTime":2000,"dataChange":true}}',
      '{"remove":{"path":"file1.parquet","deletionTimestamp":2000,"dataChange":true}}',
    ].join("\n"),
  );

  return store;
}

describe("LogReplay", () => {
  it("should build snapshot at version 0", async () => {
    const store = createStoreWithLog();
    const replay = LogReplay.create(store, "");

    const snapshot = Result.unwrap(await replay.buildSnapshot(0));
    expect(snapshot.version).toBe(0);
    expect(snapshot.activeFiles).toHaveLength(2);
    expect(snapshot.activeFiles.map((f) => f.path).sort()).toEqual([
      "file1.parquet",
      "file2.parquet",
    ]);
    expect(snapshot.protocol.minReaderVersion).toBe(1);
    expect(snapshot.metadata.id).toBe("test");
    expect(snapshot.schema.type).toBe("struct");
    expect(snapshot.schema.fields).toHaveLength(2);
  });

  it("should build latest snapshot with add/remove reconciliation", async () => {
    const store = createStoreWithLog();
    const replay = LogReplay.create(store, "");

    const snapshot = Result.unwrap(await replay.buildLatestSnapshot());
    expect(snapshot.version).toBe(1);
    expect(snapshot.activeFiles).toHaveLength(2);
    const paths = snapshot.activeFiles.map((f) => f.path).sort();
    expect(paths).toEqual(["file2.parquet", "file3.parquet"]);
  });

  it("should use latest metadata and protocol", async () => {
    const store = createStoreWithLog();
    store.addFile(
      "_delta_log/00000000000000000002.json",
      [
        `{"metaData":{"id":"updated","format":{"provider":"parquet"},"schemaString":${JSON.stringify(SCHEMA_STRING)},"partitionColumns":["name"],"configuration":{}}}`,
        '{"protocol":{"minReaderVersion":1,"minWriterVersion":3}}',
      ].join("\n"),
    );

    const replay = LogReplay.create(store, "");
    const snapshot = Result.unwrap(await replay.buildSnapshot(2));
    expect(snapshot.metadata.id).toBe("updated");
    expect(snapshot.metadata.partitionColumns).toEqual(["name"]);
    expect(snapshot.protocol.minWriterVersion).toBe(3);
  });

  it("should return error on unsupported reader version", async () => {
    const store = MockStore.create();
    store.addFile(
      "_delta_log/00000000000000000000.json",
      [
        '{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}',
        `{"metaData":{"id":"test","format":{"provider":"parquet"},"schemaString":${JSON.stringify(SCHEMA_STRING)},"partitionColumns":[],"configuration":{}}}`,
      ].join("\n"),
    );

    const replay = LogReplay.create(store, "");
    const result = await replay.buildSnapshot(0);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.message).toContain("Table requires reader version 3");
    }
  });

  it("should return error when no metadata found", async () => {
    const store = MockStore.create();
    store.addFile(
      "_delta_log/00000000000000000000.json",
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
    );

    const replay = LogReplay.create(store, "");
    const result = await replay.buildSnapshot(0);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.message).toContain("No metadata action found");
    }
  });
});
