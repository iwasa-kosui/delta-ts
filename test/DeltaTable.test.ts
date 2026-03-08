import { describe, it, expect } from "vitest";
import { DeltaTable } from "../src/DeltaTable.js";
import { MockStore } from "./helpers/MockStore.js";
import { Result } from "@praha/byethrow";

const SCHEMA_STRING = JSON.stringify({
  type: "struct",
  fields: [
    { name: "id", type: "long", nullable: false, metadata: {} },
    { name: "name", type: "string", nullable: true, metadata: {} },
    { name: "value", type: "double", nullable: true, metadata: {} },
  ],
});

function createSimpleTableStore(): MockStore {
  const store = MockStore.create();

  store.addFile(
    "_delta_log/00000000000000000000.json",
    [
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      `{"metaData":{"id":"test-table","format":{"provider":"parquet"},"schemaString":${JSON.stringify(SCHEMA_STRING)},"partitionColumns":[],"configuration":{},"createdTime":1700000000000}}`,
      '{"add":{"path":"part-00000.parquet","partitionValues":{},"size":1024,"modificationTime":1700000000000,"dataChange":true,"stats":"{\\"numRecords\\":10}"}}',
      '{"add":{"path":"part-00001.parquet","partitionValues":{},"size":2048,"modificationTime":1700000000000,"dataChange":true,"stats":"{\\"numRecords\\":20}"}}',
    ].join("\n"),
  );

  store.addFile(
    "_delta_log/00000000000000000001.json",
    [
      '{"add":{"path":"part-00002.parquet","partitionValues":{},"size":512,"modificationTime":1700000001000,"dataChange":true,"stats":"{\\"numRecords\\":5}"}}',
      '{"remove":{"path":"part-00000.parquet","deletionTimestamp":1700000001000,"dataChange":true}}',
    ].join("\n"),
  );

  return store;
}

describe("DeltaTable", () => {
  it("should open a table and return metadata", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
    expect(table.metadata().id).toBe("test-table");
    expect(table.protocol().minReaderVersion).toBe(1);
  });

  it("should return schema", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const schema = table.schema();
    expect(schema.type).toBe("struct");
    expect(schema.fields).toHaveLength(3);
    expect(schema.fields[0].name).toBe("id");
    expect(schema.fields[0].type).toBe("long");
  });

  it("should return active files after add/remove", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const paths = table.filePaths().sort();
    expect(paths).toEqual(["part-00001.parquet", "part-00002.parquet"]);
  });

  it("should compute numRecords from file stats", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.numRecords()).toBe(25);
  });

  it("should return partition columns", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.partitionColumns()).toEqual([]);
  });

  it("should load a specific version with atVersion()", async () => {
    const store = createSimpleTableStore();
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const v0 = Result.unwrap(await table.atVersion(0));
    expect(v0.version()).toBe(0);
    expect(v0.filePaths().sort()).toEqual([
      "part-00000.parquet",
      "part-00001.parquet",
    ]);
    expect(v0.numRecords()).toBe(30);
  });

  it("should return error when neither store nor url is provided", async () => {
    const result = await DeltaTable.open({});
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.message).toContain(
        "Either 'store' or 'url' must be provided",
      );
    }
  });
});
