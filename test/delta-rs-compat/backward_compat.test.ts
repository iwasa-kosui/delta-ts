/**
 * Tests ported from delta-rs: backward compatibility with various Delta versions
 * Tests: delta-0.2.0, delta-0.8.0, delta-0.8.0-*
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("backward compatibility (delta-rs compat)", () => {
  it("should read delta-0.2.0 table (legacy format)", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.2.0`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(3);
    expect(table.protocol().minReaderVersion).toBe(1);

    const schema = table.schema();
    expect(schema.fields.length).toBeGreaterThan(0);
    expect(schema.fields.some((f) => f.name === "value")).toBe(true);
  });

  it("should read delta-0.8.0 table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8.0`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);
  });

  it("should read delta-0.8.0-date table (date type support)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-date`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(2);

    const dateField = schema.fields.find((f) => f.name === "date");
    expect(dateField).toBeDefined();
    expect(dateField!.type).toBe("date");
  });

  it("should read delta-0.8.0-partitioned table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-partitioned`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["year", "month", "day"]);
    expect(table.files()).toHaveLength(6);
  });

  it("should read delta-0.8.0-null-partition table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-null-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["k"]);
    expect(table.files()).toHaveLength(2);
  });

  it("should read delta-0.8.0-numeric-partition table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-numeric-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["x", "y"]);
    expect(table.files()).toHaveLength(2);
  });

  it("should read delta-0.8.0-special-partition table", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-special-partition`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.partitionColumns()).toEqual(["x"]);
    expect(table.files()).toHaveLength(2);
  });

  it("should read delta-0.8-empty table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.8-empty`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);
    expect(table.files()).toHaveLength(0);
  });
});
