/**
 * Tests ported from delta-rs: golden/data-reader-array-primitives,
 * table_with_edge_timestamps, delta-0.8.0-date, http_requests
 * Tests various schema types and data type support
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import type { ArrayType, StructType, MapType } from "../../src/types/schema.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("schema types (delta-rs compat)", () => {
  it("should read golden/data-reader-array-primitives with array types", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/golden/data-reader-array-primitives`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(10);

    const fieldNames = schema.fields.map((f) => f.name);
    expect(fieldNames).toEqual([
      "as_array_int",
      "as_array_long",
      "as_array_byte",
      "as_array_short",
      "as_array_boolean",
      "as_array_float",
      "as_array_double",
      "as_array_string",
      "as_array_binary",
      "as_array_big_decimal",
    ]);

    // All fields should be array types
    for (const field of schema.fields) {
      const arrayType = field.type as ArrayType;
      expect(arrayType.type).toBe("array");
      expect(arrayType.containsNull).toBe(true);
    }

    // Check specific element types
    const intArrayField = schema.fields[0].type as ArrayType;
    expect(intArrayField.elementType).toBe("integer");

    const longArrayField = schema.fields[1].type as ArrayType;
    expect(longArrayField.elementType).toBe("long");

    const boolArrayField = schema.fields[4].type as ArrayType;
    expect(boolArrayField.elementType).toBe("boolean");

    const stringArrayField = schema.fields[7].type as ArrayType;
    expect(stringArrayField.elementType).toBe("string");
  });

  it("should read table_with_edge_timestamps", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table_with_edge_timestamps`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(3);

    const fieldMap = Object.fromEntries(
      schema.fields.map((f) => [f.name, f.type]),
    );
    expect(fieldMap["BIG_DATE"]).toBe("timestamp");
    expect(fieldMap["NORMAL_DATE"]).toBe("timestamp");
    expect(fieldMap["SOME_VALUE"]).toBe("integer");

    expect(table.files()).toHaveLength(2);
  });

  it("should read delta-0.8.0-date with date type", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-date`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);

    const schema = table.schema();
    expect(schema.fields).toHaveLength(2);

    const fieldMap = Object.fromEntries(
      schema.fields.map((f) => [f.name, f.type]),
    );
    expect(fieldMap["date"]).toBe("date");
    expect(fieldMap["dayOfYear"]).toBe("integer");

    expect(table.files()).toHaveLength(1);
  });

  it("should read http_requests table with named metadata", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/http_requests`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(1);

    const metadata = table.metadata();
    expect(metadata.name).toBe("http_requests");
    expect(metadata.partitionColumns).toEqual(["date"]);

    const schema = table.schema();
    const fieldNames = schema.fields.map((f) => f.name);
    expect(fieldNames).toContain("ClientIP");
    expect(fieldNames).toContain("EdgeResponseBytes");
    expect(fieldNames).toContain("EdgeResponseStatus");

    const fieldMap = Object.fromEntries(
      schema.fields.map((f) => [f.name, f.type]),
    );
    expect(fieldMap["EdgeResponseBytes"]).toBe("long");
    expect(fieldMap["EdgeResponseStatus"]).toBe("short");
    expect(fieldMap["EdgeEndTimestamp"]).toBe("timestamp");
  });

  it("should read checkpoints table with nested struct schema", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/checkpoints`);
    const table = Result.unwrap(await DeltaTable.open({ store }));

    const schema = table.schema();
    const someStructField = schema.fields.find(
      (f) => f.name === "some_struct",
    );
    expect(someStructField).toBeDefined();

    const structType = someStructField!.type as StructType;
    expect(structType.type).toBe("struct");
    expect(structType.fields).toHaveLength(2);
    expect(structType.fields[0].name).toBe("some_struct_member");
    expect(structType.fields[0].type).toBe("string");
    expect(structType.fields[1].name).toBe("some_struct_timestamp");
    expect(structType.fields[1].type).toBe("timestamp");
  });
});
