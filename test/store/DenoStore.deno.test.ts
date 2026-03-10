import {
  assertEquals,
  assertStrictEquals,
} from "https://deno.land/std@0.224.0/assert/mod.ts";
import { Result } from "@praha/byethrow";
import { DenoStore } from "../../src/store-deno/DenoStore.ts";

let tempDir: string;
let store: ReturnType<typeof DenoStore.create>;

async function setup() {
  tempDir = await Deno.makeTempDir({ prefix: "denostore-test-" });
  store = DenoStore.create(tempDir);

  await Deno.writeTextFile(`${tempDir}/hello.txt`, "Hello, world!");
  await Deno.writeFile(
    `${tempDir}/binary.bin`,
    new Uint8Array([0, 1, 2, 3, 4]),
  );
  await Deno.mkdir(`${tempDir}/subdir`);
  await Deno.writeTextFile(`${tempDir}/subdir/a.txt`, "a");
  await Deno.writeTextFile(`${tempDir}/subdir/b.txt`, "b");
}

async function teardown() {
  await Deno.remove(tempDir, { recursive: true });
}

// read

Deno.test("read - reads a text file", async () => {
  await setup();
  try {
    const result = await store.read("hello.txt");
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertStrictEquals(result.value, "Hello, world!");
    }
  } finally {
    await teardown();
  }
});

Deno.test("read - returns failure for missing file", async () => {
  await setup();
  try {
    const result = await store.read("nonexistent.txt");
    assertStrictEquals(Result.isFailure(result), true);
    if (Result.isFailure(result)) {
      assertStrictEquals(result.error.type, "STORE_ERROR");
    }
  } finally {
    await teardown();
  }
});

// readBytes

Deno.test("readBytes - reads full file as ArrayBuffer", async () => {
  await setup();
  try {
    const result = await store.readBytes("binary.bin");
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertEquals(
        new Uint8Array(result.value),
        new Uint8Array([0, 1, 2, 3, 4]),
      );
    }
  } finally {
    await teardown();
  }
});

Deno.test("readBytes - reads a byte range", async () => {
  await setup();
  try {
    const result = await store.readBytes("binary.bin", 1, 4);
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertEquals(new Uint8Array(result.value), new Uint8Array([1, 2, 3]));
    }
  } finally {
    await teardown();
  }
});

Deno.test("readBytes - returns failure for missing file", async () => {
  await setup();
  try {
    const result = await store.readBytes("nonexistent.bin");
    assertStrictEquals(Result.isFailure(result), true);
    if (Result.isFailure(result)) {
      assertStrictEquals(result.error.type, "STORE_ERROR");
    }
  } finally {
    await teardown();
  }
});

// list

Deno.test("list - lists directory entries sorted", async () => {
  await setup();
  try {
    const result = await store.list("subdir");
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertEquals(result.value, ["a.txt", "b.txt"]);
    }
  } finally {
    await teardown();
  }
});

Deno.test("list - returns empty array for nonexistent directory", async () => {
  await setup();
  try {
    const result = await store.list("nonexistent");
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertEquals(result.value, []);
    }
  } finally {
    await teardown();
  }
});

// exists

Deno.test("exists - returns true for existing file", async () => {
  await setup();
  try {
    assertStrictEquals(await store.exists("hello.txt"), true);
  } finally {
    await teardown();
  }
});

Deno.test("exists - returns false for nonexistent file", async () => {
  await setup();
  try {
    assertStrictEquals(await store.exists("nonexistent.txt"), false);
  } finally {
    await teardown();
  }
});

// fileSize

Deno.test("fileSize - returns file size", async () => {
  await setup();
  try {
    const result = await store.fileSize("binary.bin");
    assertStrictEquals(Result.isSuccess(result), true);
    if (Result.isSuccess(result)) {
      assertStrictEquals(result.value, 5);
    }
  } finally {
    await teardown();
  }
});

Deno.test("fileSize - returns failure for missing file", async () => {
  await setup();
  try {
    const result = await store.fileSize("nonexistent.bin");
    assertStrictEquals(Result.isFailure(result), true);
    if (Result.isFailure(result)) {
      assertStrictEquals(result.error.type, "STORE_ERROR");
    }
  } finally {
    await teardown();
  }
});
