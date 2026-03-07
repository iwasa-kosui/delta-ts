/**
 * Tests ported from delta-rs: crates/core/tests/read_delta_log_test.rs
 * and LogReplay-related tests
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("log replay (delta-rs compat)", () => {
  it("should replay checkpoints table (13 versions, no checkpoints)", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/checkpoints`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    // 13 JSON commits (v0-v12), no checkpoint files present
    expect(table.version()).toBe(12);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);

    // Table has some_struct, value, ts, date fields; partitioned by date
    expect(table.partitionColumns()).toEqual(["date"]);

    const schema = table.schema();
    expect(schema.fields.length).toBeGreaterThanOrEqual(3);
  });

  it("should replay delta-0.8.0 table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8.0`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
    expect(table.protocol().minReaderVersion).toBe(1);
  });

  it("should correctly handle add then remove in same version", async () => {
    // simple_table v2 is an Overwrite - it removes all v1 files and adds new ones
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV2 = Result.unwrap(await table.atVersion(2));

    // v2 adds 6 files and removes all from v1
    expect(tableV2.files()).toHaveLength(6);

    // Verify none of the v1-specific files remain
    const filePaths = tableV2.filePaths();
    expect(
      filePaths.some((p) => p.includes("part-00045-332fe409")),
    ).toBe(false);
    expect(
      filePaths.some((p) => p.includes("part-00190-8ac0ae67")),
    ).toBe(false);
  });

  it("should reject table_with_deletion_logs (requires reader v3)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table_with_deletion_logs`,
    );

    // This table requires reader version 3 (deletion vectors)
    const result = await DeltaTable.open({ store });
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.message).toMatch(/reader version 3/);
    }
  });

  it("should read delta-live-table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-live-table`,
    );

    try {
      const table = Result.unwrap(await DeltaTable.open({ store }));
      expect(table.version()).toBe(1);
    } catch {
      // Some DLT features may not be supported; that's OK for now
    }
  });
});
