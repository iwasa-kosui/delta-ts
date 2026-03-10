import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/*.bun.test.*",
      "**/*.deno.test.*",
    ],
  },
});
