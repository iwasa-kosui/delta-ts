/**
 * Tests ported from delta-rs: crates/core/tests/commit_info_format.rs
 * Tests commit info action parsing
 */
import { describe, it, expect } from "vitest";
import { LogReader } from "../../src/log/LogReader.js";
import { FileSystemStore } from "../helpers/FileSystemStore.js";
import { Result } from "@praha/byethrow";

const DELTA_RS_TEST_DATA = "/tmp/delta-rs-tests/crates/test/tests/data";

describe("commit info (delta-rs compat)", () => {
  it("should parse commit info from simple_table v0", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.operation).toBe("WRITE");
      expect(commitInfo.commitInfo.timestamp).toBe(1587968586154);
      expect(commitInfo.commitInfo.isBlindAppend).toBe(true);
    }
  });

  it("should parse commit info operation parameters", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.operationParameters).toBeDefined();
      expect(commitInfo.commitInfo.operationParameters!["mode"]).toBe(
        "ErrorIfExists",
      );
    }
  });

  it("should parse MERGE commit info from simple_table v1", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(1));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.operation).toBe("MERGE");
      expect(commitInfo.commitInfo.readVersion).toBe(0);
      expect(commitInfo.commitInfo.isBlindAppend).toBe(false);
    }
  });

  it("should parse DELETE commit info from simple_table v4", async () => {
    const store = FileSystemStore.create(`${DELTA_RS_TEST_DATA}/simple_table`);
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(4));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.operation).toBe("DELETE");
      expect(commitInfo.commitInfo.readVersion).toBe(3);
    }
  });

  it("should parse commit info with operation metrics", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/delta-0.8.0-partitioned`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.operationMetrics).toBeDefined();
      expect(commitInfo.commitInfo.operationMetrics!["numFiles"]).toBe("6");
      expect(commitInfo.commitInfo.operationMetrics!["numOutputRows"]).toBe(
        "7",
      );
    }
  });

  it("should parse commit info with engine info", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/table-with-dv-small`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      expect(commitInfo.commitInfo.engineInfo).toBe(
        "Databricks-Runtime/<unknown>",
      );
      expect(commitInfo.commitInfo.txnId).toBeDefined();
      expect(commitInfo.commitInfo.isolationLevel).toBe("WriteSerializable");
    }
  });

  it("should parse delta-rs format commit info from http_requests", async () => {
    const store = FileSystemStore.create(
      `${DELTA_RS_TEST_DATA}/http_requests`,
    );
    const reader = LogReader.create(store, "");

    const actions = Result.unwrap(await reader.readVersion(0));
    const commitInfo = actions.find((a) => a.type === "commitInfo");

    expect(commitInfo).toBeDefined();
    if (commitInfo?.type === "commitInfo") {
      // http_requests uses non-standard commitInfo with "delta-rs" key
      expect(commitInfo.commitInfo.timestamp).toBe(1681604880998);
    }
  });
});
