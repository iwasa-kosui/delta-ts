/**
 * Tests ported from delta-rs: delta-0.8-empty fixture
 * Tests reading a table that becomes empty after all files are deleted
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("empty table (delta-rs compat)", () => {
  it("should read delta-0.8-empty at v0 (before delete)", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8-empty`);
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV0 = Result.unwrap(await table.atVersion(0));

    expect(tableV0.version()).toBe(0);
    expect(tableV0.files()).toHaveLength(2);

    const schema = tableV0.schema();
    expect(schema.fields).toHaveLength(1);
    expect(schema.fields[0].name).toBe("column");
    expect(schema.fields[0].type).toBe("long");
  });

  it("should read delta-0.8-empty at v1 (after delete - empty table)", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8-empty`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
    expect(table.files()).toHaveLength(0);
    expect(table.filePaths()).toEqual([]);

    // Schema should still be available even though table is empty
    const schema = table.schema();
    expect(schema.fields).toHaveLength(1);
    expect(schema.fields[0].name).toBe("column");
  });

  it("should return 0 numRecords for empty table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8-empty`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    // Empty table with no files should return 0
    expect(table.numRecords()).toBe(0);
  });
});
