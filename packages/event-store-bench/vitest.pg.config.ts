import { defineConfig } from "vitest/config"
import path from "path"

export default defineConfig({
    resolve: {
        alias: {
            "@dcb-es/event-store": path.resolve(__dirname, "../event-store/index.ts"),
            "@dcb-es/event-store-postgres": path.resolve(__dirname, "../event-store-postgres/index.ts"),
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
