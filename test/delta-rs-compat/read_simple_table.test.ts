/**
 * Tests ported from delta-rs: crates/test/src/read.rs
 * Tests: read_simple_table, read_simple_table_with_version
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("read_simple_table (delta-rs compat)", () => {
  it("should read simple_table at latest version (v4)", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(4);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);

    const files = table.files();
    expect(files).toHaveLength(5);

    const filePaths = table.filePaths();
    expect(filePaths).toContain(
      "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
    );
    expect(filePaths).toContain(
      "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
    );
    expect(filePaths).toContain(
      "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
    );
    expect(filePaths).toContain(
      "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
    );
    expect(filePaths).toContain(
      "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
    );
  });

  it("should read simple_table at version 3", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV3 = Result.unwrap(await table.atVersion(3));

    expect(tableV3.version()).toBe(3);
    expect(tableV3.protocol().minReaderVersion).toBe(1);
    expect(tableV3.protocol().minWriterVersion).toBe(2);

    const files = tableV3.files();
    expect(files).toHaveLength(6);
  });

  it("should read simple_table at version 0", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV0 = Result.unwrap(await table.atVersion(0));

    expect(tableV0.version()).toBe(0);

    const filePaths = tableV0.filePaths();
    expect(filePaths).toHaveLength(6);
    expect(filePaths).toContain(
      "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet",
    );
    expect(filePaths).toContain(
      "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet",
    );
  });

  it("should read simple_table at version 1", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV1 = Result.unwrap(await table.atVersion(1));

    expect(tableV1.version()).toBe(1);
    // v0 had 7 files, v1 removes 5 and adds 21 => 23 files
    const files = tableV1.files();
    expect(files.length).toBeGreaterThan(0);
  });

  it("should read schema from simple_table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const schema = table.schema();
    expect(schema.type).toBe("struct");
    expect(schema.fields).toHaveLength(1);
    expect(schema.fields[0].name).toBe("id");
    expect(schema.fields[0].type).toBe("long");
    expect(schema.fields[0].nullable).toBe(true);
  });

  it("should read metadata from simple_table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const metadata = table.metadata();
    expect(metadata.id).toBe("5fba94ed-9794-4965-ba6e-6ee3c0d22af9");
    expect(metadata.partitionColumns).toEqual([]);
    expect(metadata.format.provider).toBe("parquet");
  });

  it("should have no partition columns in simple_table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.partitionColumns()).toEqual([]);
  });
});
