import { Pool } from "pg"
import "source-map-support/register"
import { startCli } from "./src/Cli"
import { installPostgresCourseSubscriptionsRepository } from "./src/postgresCourseSubscriptionRepository/PostgresCourseSubscriptionRespository"
import { Api, setupHandlers } from "./src/api/Api"
import { HandlerCatchup, PostgresEventStore } from "@dcb-es/event-store-postgres"
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

    const handlerCatchup = new HandlerCatchup(pool, eventStore)
    await handlerCatchup.ensureInstalled(Object.keys(setupHandlers(pool)))

    await installPostgresCourseSubscriptionsRepository(pool)
    const api = new Api(pool, eventStore, handlerCatchup)

    await startCli(api)
})()
