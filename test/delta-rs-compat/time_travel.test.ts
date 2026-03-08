/**
 * Tests ported from delta-rs: crates/core/tests/time_travel.rs
 * Tests: time travel by version (timestamp-based time travel is not applicable as it depends on file mtime)
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("time_travel (delta-rs compat)", () => {
  it("should open simple_table at each version and verify file counts", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    // Version 0: 6 files (initial WRITE)
    const v0 = Result.unwrap(await table.atVersion(0));
    expect(v0.version()).toBe(0);
    expect(v0.files()).toHaveLength(6);

    // Version 1: MERGE - removes some v0 files, adds new files
    const v1 = Result.unwrap(await table.atVersion(1));
    expect(v1.version()).toBe(1);
    expect(v1.files().length).toBeGreaterThan(0);

    // Version 2: Overwrite - removes all v1 files, adds 6 files
    const v2 = Result.unwrap(await table.atVersion(2));
    expect(v2.version()).toBe(2);
    expect(v2.files()).toHaveLength(6);

    // Version 3: UPDATE - removes 2, adds 2 => 6 files
    const v3 = Result.unwrap(await table.atVersion(3));
    expect(v3.version()).toBe(3);
    expect(v3.files()).toHaveLength(6);

    // Version 4: DELETE - removes 2, adds 1 => 5 files
    const v4 = Result.unwrap(await table.atVersion(4));
    expect(v4.version()).toBe(4);
    expect(v4.files()).toHaveLength(5);
  });

  it("should maintain consistent schema across versions", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    for (let v = 0; v <= 4; v++) {
      const tableAtVersion = Result.unwrap(await table.atVersion(v));
      const schema = tableAtVersion.schema();
      expect(schema.fields).toHaveLength(1);
      expect(schema.fields[0].name).toBe("id");
      expect(schema.fields[0].type).toBe("long");
    }
  });

  it("should maintain consistent protocol across versions", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    for (let v = 0; v <= 4; v++) {
      const tableAtVersion = Result.unwrap(await table.atVersion(v));
      expect(tableAtVersion.protocol().minReaderVersion).toBe(1);
      expect(tableAtVersion.protocol().minWriterVersion).toBe(2);
    }
  });
});
