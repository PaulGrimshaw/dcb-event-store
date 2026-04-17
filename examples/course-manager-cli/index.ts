import { Pool } from "pg"
import "source-map-support/register"
import { startCli } from "./src/Cli.js"
import { Api } from "./src/api/Api.js"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"
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

    const api = new Api(eventStore)

    await startCli(api)
})()
