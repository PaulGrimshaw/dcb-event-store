import { Pool } from "pg"
import "source-map-support/register"
import { startCli } from "./src/Cli.js"
import { installPostgresCourseSubscriptionsRepository } from "./src/postgresCourseSubscriptionRepository/PostgresCourseSubscriptionRespository.js"
import { Api, PROJECTION_NAME } from "./src/api/Api.js"
import { PostgresEventStore, runHandler, ensureHandlersInstalled } from "@dcb-es/event-store-postgres"
import { PostgresCourseSubscriptionsProjection } from "./src/api/PostgresCourseSubscriptionsProjection.js"
;(async () => {
    const postgresConfig = {
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        database: "dcb_test_1"
    }

    const pool = new Pool(postgresConfig)
    const eventStore = new PostgresEventStore({ pool })
    await eventStore.ensureInstalled()
    await ensureHandlersInstalled(pool, [PROJECTION_NAME], "_handler_bookmarks")
    await installPostgresCourseSubscriptionsRepository(pool)

    // Start projection handler in background
    const controller = new AbortController()
    const { promise: handlerPromise } = runHandler({
        pool,
        eventStore,
        handlerName: PROJECTION_NAME,
        handlerFactory: client => PostgresCourseSubscriptionsProjection(client),
        signal: controller.signal
    })

    handlerPromise.catch(err => {
        console.error("Projection handler error:", err)
        process.exit(1)
    })

    const api = new Api(pool, eventStore)
    await startCli(api)

    // Shutdown
    controller.abort()
    await handlerPromise.catch(() => {})
    await pool.end()
})()
