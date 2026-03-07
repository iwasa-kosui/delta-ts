/**
 * Tests ported from delta-rs: LogReader-related tests
 * Tests: version listing, log parsing, _last_checkpoint reading
 */
import { describe, it, expect } from "vitest";
import { LogReader } from "../../src/log/LogReader.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("log reader with real fixtures (delta-rs compat)", () => {
  it("should list versions for simple_table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const versions = Result.unwrap(await reader.listVersions());
    expect(versions).toEqual([0, 1, 2, 3, 4]);
  });

  it("should find latest version for simple_table", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const latest = Result.unwrap(await reader.latestVersion());
    expect(latest).toBe(4);
  });

  it("should return VERSION_NOT_FOUND for non-existent version", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const result = await reader.readVersion(99);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.error.type).toBe("VERSION_NOT_FOUND");
    }
  });

  it("should read _last_checkpoint from simple_table_with_checkpoint", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/simple_table_with_checkpoint`,
    );
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).not.toBeNull();
    expect(checkpoint!.version).toBe(10);
    expect(checkpoint!.size).toBe(13);
  });

  it("should read _last_checkpoint from delta-0.2.0", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/delta-0.2.0`);
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).not.toBeNull();
    expect(checkpoint!.version).toBe(3);
    expect(checkpoint!.size).toBe(10);
  });

  it("should return null for _last_checkpoint when file doesn't exist", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).toBeNull();
  });

  it("should list versions for checkpoints_vacuumed (starts at v5)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/checkpoints_vacuumed`,
    );
    const reader = LogReader.create(store, "");

    const versions = Result.unwrap(await reader.listVersions());
    // Only JSON versions (not checkpoint parquet files)
    expect(versions).toContain(5);
    expect(versions).toContain(12);
    expect(versions).not.toContain(0);
    expect(versions).not.toContain(4);
  });

  it("should parse all action types from simple_table v0", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));

    const commitInfos = actions.filter((a) => a.type === "commitInfo");
    const protocols = actions.filter((a) => a.type === "protocol");
    const metadatas = actions.filter((a) => a.type === "metaData");
    const adds = actions.filter((a) => a.type === "add");

    expect(commitInfos).toHaveLength(1);
    expect(protocols).toHaveLength(1);
    expect(metadatas).toHaveLength(1);
    expect(adds).toHaveLength(6);
  });

  it("should parse remove actions from simple_table v1", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(1));

    const removes = actions.filter((a) => a.type === "remove");
    const adds = actions.filter((a) => a.type === "add");

    expect(removes).toHaveLength(5);
    expect(adds).toHaveLength(21);

    // Verify remove action structure
    for (const action of removes) {
      if (action.type === "remove") {
        expect(action.remove.path).toBeDefined();
        expect(action.remove.deletionTimestamp).toBeDefined();
        expect(action.remove.dataChange).toBe(true);
      }
    }
  });

  it("should list versions for simple_table_with_checkpoint (v0-v10)", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/simple_table_with_checkpoint`,
    );
    const reader = LogReader.create(store, "");

    const versions = Result.unwrap(await reader.listVersions());
    // Only JSON versions are listed (v0-v10)
    expect(versions).toContain(0);
    expect(versions).toContain(10);
  });

  it("should read _last_checkpoint from latest_not_checkpointed", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/latest_not_checkpointed`,
    );
    const reader = LogReader.create(store, "");

    const checkpoint = await reader.getLastCheckpoint();
    expect(checkpoint).not.toBeNull();
    expect(checkpoint!.version).toBe(2);
    expect(checkpoint!.size).toBe(1);
  });
});
