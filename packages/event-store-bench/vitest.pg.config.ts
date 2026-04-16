import { defineConfig } from "vitest/config"
import path from "path"

export default defineConfig({
    resolve: {
        alias: {
            "@test": path.resolve(__dirname, "../../test"),
        },
    },
    test: {
        globals: true,
        include: ["src/pg.tests.ts"],
        testTimeout: 120_000,
        globalSetup: "../../test/vitest.globalSetup.ts",
        pool: "forks",
        poolOptions: { forks: { singleFork: true } },
    },
})
