/**
 * Tests ported from delta-rs: crates/core/tests/read_delta_log_test.rs
 * Tests: test_read_table_features, read_delta_table_with_null_stats_in_notnull_struct
 */
import { describe, it, expect } from "vitest";
import { DeltaTable } from "../../src/DeltaTable.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("table features (delta-rs compat)", () => {
  it("should read simple_table_features with reader/writer features", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/simple_table_features`,
    );

    // This table has minReaderVersion=5, which is above our supported version (1)
    const result = await DeltaTable.open({ store });
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.type).toBe("UNSUPPORTED_PROTOCOL");
    }
  });

  it("should read table_with_edge_timestamps (protocol v1)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table_with_edge_timestamps`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.version()).toBe(0);
    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(2);
  });

  it("should reject table-with-dv-small (requires reader v3 for deletion vectors)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );

    // minReaderVersion=3 with deletionVectors feature
    const result = await DeltaTable.open({ store });
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.type).toBe("UNSUPPORTED_PROTOCOL");
    }
  });

  it("should read http_requests with minWriterVersion=1", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/http_requests`,
    );
    const table = Result.unwrap(await DeltaTable.open({ store }));

    expect(table.protocol().minReaderVersion).toBe(1);
    expect(table.protocol().minWriterVersion).toBe(1);
  });
});
