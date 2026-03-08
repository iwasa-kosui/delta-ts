/**
 * Tests ported from delta-rs: table-with-dv-small fixture
 * Tests deletion vector metadata parsing (not DV application)
 */
import { describe, it, expect } from "vitest";
import { LogReader } from "../../src/log/LogReader.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("deletion vectors metadata (delta-rs compat)", () => {
  it("should parse deletion vector in add action at v1", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(1));

    const addAction = actions.find((a) => a.type === "add");
    expect(addAction).toBeDefined();
    if (addAction?.type === "add") {
      expect(addAction.add.deletionVector).toBeDefined();

      const dv = addAction.add.deletionVector!;
      expect(dv.storageType).toBe("u");
      expect(dv.pathOrInlineDv).toBe("vBn[lx{q8@P<9BNH/isA");
      expect(dv.offset).toBe(1);
      expect(dv.sizeInBytes).toBe(36);
      expect(dv.cardinality).toBe(2);
    }
  });

  it("should parse remove action without deletion vector at v1", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(1));

    const removeAction = actions.find((a) => a.type === "remove");
    expect(removeAction).toBeDefined();
    if (removeAction?.type === "remove") {
      expect(removeAction.remove.path).toBe(
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
      );
      expect(removeAction.remove.extendedFileMetadata).toBe(true);
    }
  });

  it("should parse v0 stats with tightBounds=true", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const addAction = actions.find((a) => a.type === "add");

    expect(addAction).toBeDefined();
    if (addAction?.type === "add") {
      expect(addAction.add.stats).toBeDefined();

      const stats = JSON.parse(addAction.add.stats!);
      expect(stats.numRecords).toBe(10);
      expect(stats.minValues.value).toBe(0);
      expect(stats.maxValues.value).toBe(9);
      expect(stats.tightBounds).toBe(true);
    }
  });

  it("should parse v1 stats with tightBounds=false (after DV application)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(1));
    const addAction = actions.find((a) => a.type === "add");

    expect(addAction).toBeDefined();
    if (addAction?.type === "add") {
      expect(addAction.add.stats).toBeDefined();

      const stats = JSON.parse(addAction.add.stats!);
      expect(stats.numRecords).toBe(10);
      expect(stats.tightBounds).toBe(false);
    }
  });

  it("should parse table-without-dv-small for comparison", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-without-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const addAction = actions.find((a) => a.type === "add");

    expect(addAction).toBeDefined();
    if (addAction?.type === "add") {
      expect(addAction.add.deletionVector).toBeUndefined();
    }
  });
});
