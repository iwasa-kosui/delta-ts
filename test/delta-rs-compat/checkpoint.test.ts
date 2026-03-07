/**
 * Tests ported from delta-rs: crates/core/tests/integration_checkpoint.rs
 * and related checkpoint tests
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("checkpoint reading (delta-rs compat)", () => {
  it("should read simple_table_with_checkpoint at latest version (v10)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/simple_table_with_checkpoint`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(10);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(1);
    expect(schema.fields[0].name).toBe("version");
    expect(schema.fields[0].type).toBe("integer");
  });

  it("should read delta-0.2.0 table with checkpoint at v3", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.2.0`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(3);
    expect(table.protocol().minReaderVersion).toBe(1);

    const schema = table.schema();
    expect(schema.fields.length).toBeGreaterThan(0);
  });

  it("should read latest_not_checkpointed (latest version beyond last checkpoint)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/latest_not_checkpointed`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    // Latest version is 3, checkpoint is at v2
    expect(table.version()).toBe(3);
  });

  it.fails("should read checkpoints_vacuumed table from checkpoint (requires checkpoint discovery without _last_checkpoint)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/checkpoints_vacuumed`,
    );
    // This table has versions 5-12 only, with checkpoints at v5 and v10
    // No _last_checkpoint file => delta-ts cannot discover checkpoints
    // Without checkpoint, metadata is not found since v0-v4 are vacuumed
    // delta-rs handles this by scanning for checkpoint parquet files
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(12);
  });

  it("should read table at version with checkpoint at v10 (simple_table_with_checkpoint at v10)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/simple_table_with_checkpoint`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));
    const tableV10 = Result.unwrap(await table.atVersion(10));

    expect(tableV10.version()).toBe(10);
  });

  it("should read python-0.25.5-checkpoint (older Python-generated checkpoint)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/python-0.25.5-checkpoint`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
  });
});
