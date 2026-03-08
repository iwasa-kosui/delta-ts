/**
 * Tests ported from delta-rs: crates/core/tests/read_delta_partitions_test.rs
 * and crates/test/src/read.rs (read_encoded_table)
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("read_delta_partitions (delta-rs compat)", () => {
  it("should read delta-0.8.0-partitioned table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-partitioned`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);

    const files = table.files();
    expect(files).toHaveLength(6);

    expect(table.partitionColumns()).toEqual(["year", "month", "day"]);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(4);
    expect(schema.fields.map((f) => f.name)).toEqual([
      "value",
      "year",
      "month",
      "day",
    ]);
  });

  it("should read partition values from partitioned table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-partitioned`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const files = table.files();
    const partitionValues = files.map((f) => f.partitionValues);

    // Check specific partition values exist
    expect(partitionValues).toContainEqual({
      year: "2020",
      month: "1",
      day: "1",
    });
    expect(partitionValues).toContainEqual({
      year: "2020",
      month: "2",
      day: "3",
    });
    expect(partitionValues).toContainEqual({
      year: "2021",
      month: "12",
      day: "20",
    });
  });

  it("should read null partition values from delta-0.8.0-null-partition", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-null-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["k"]);

    const files = table.files();
    expect(files).toHaveLength(2);

    const partitionValues = files.map((f) => f.partitionValues);
    expect(partitionValues).toContainEqual({ k: "A" });
    expect(partitionValues).toContainEqual({ k: null });
  });

  it("should read numeric partition values from delta-0.8.0-numeric-partition", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-numeric-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["x", "y"]);

    const files = table.files();
    expect(files).toHaveLength(2);

    const partitionValues = files.map((f) => f.partitionValues);
    expect(partitionValues).toContainEqual({ x: "9", y: "9.9" });
    expect(partitionValues).toContainEqual({ x: "10", y: "10.0" });
  });

  it("should read URL-encoded special partition values", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-special-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["x"]);

    const files = table.files();
    expect(files).toHaveLength(2);

    const partitionValues = files.map((f) => f.partitionValues);
    expect(partitionValues).toContainEqual({ x: "A/A" });
    expect(partitionValues).toContainEqual({ x: "B B" });

    // Also verify file paths contain URL-encoded values
    const filePaths = table.filePaths();
    expect(filePaths.some((p) => p.includes("x=A%252FA"))).toBe(true);
    expect(filePaths.some((p) => p.includes("x=B%2520B"))).toBe(true);
  });

  it("should read delta-2.2.0-partitioned-types table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-2.2.0-partitioned-types`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["c1", "c2"]);

    const schema = table.schema();
    const fieldNames = schema.fields.map((f) => f.name);
    expect(fieldNames).toContain("c1");
    expect(fieldNames).toContain("c2");
    expect(fieldNames).toContain("c3");
  });
});
