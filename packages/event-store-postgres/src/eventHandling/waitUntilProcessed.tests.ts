import { Pool } from "pg"
import { DcbEvent, SequencePosition, Tags } from "@dcb-es/event-store"
import { PostgresEventStore } from "../eventStore/PostgresEventStore"
import { runHandler } from "./runHandler"
import { waitUntilProcessed } from "./waitUntilProcessed"
import { WaitTimeoutError } from "./WaitTimeoutError"
import { ensureHandlersInstalled } from "./ensureHandlersInstalled"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const event = (type: string): DcbEvent => ({
    type,
    tags: Tags.fromObj({ e: "1" }),
    data: {},
    metadata: {}
})

const HANDLER = "TestHandler"

describe("waitUntilProcessed", () => {
    let pool: Pool
    let store: PostgresEventStore

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 20 })
        store = new PostgresEventStore({ pool })
        await store.ensureInstalled()
        await ensureHandlersInstalled(pool, [HANDLER], "_handler_bookmarks")
    })

    afterEach(async () => {
        await pool.query("TRUNCATE table events")
        await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
        await pool.query("UPDATE _handler_bookmarks SET last_sequence_position = 0")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    test("returns immediately when bookmark already past position", async () => {
        // Manually set bookmark ahead
        await pool.query("UPDATE _handler_bookmarks SET last_sequence_position = 10 WHERE handler_id = $1", [HANDLER])

        const start = Date.now()
        await waitUntilProcessed(pool, HANDLER, SequencePosition.fromString("5"))
        expect(Date.now() - start).toBeLessThan(100)
    })

    test("waits and returns when handler advances bookmark", async () => {
        const controller = new AbortController()

        // Start handler in background
        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER,
            handlerFactory: () => ({
                when: {
                    Evt: async () => {}
                }
            }),
            signal: controller.signal
        })

        // Append event
        const position = await store.append({ events: event("Evt") })

        // Wait for handler to process it
        await waitUntilProcessed(pool, HANDLER, position)

        // Verify bookmark advanced
        const bookmark = await pool.query(
            "SELECT last_sequence_position FROM _handler_bookmarks WHERE handler_id = $1",
            [HANDLER]
        )
        expect(Number(bookmark.rows[0].last_sequence_position)).toBe(1)

        controller.abort()
        await promise.catch(() => {})
    })

    test("throws WaitTimeoutError on timeout", async () => {
        await store.append({ events: event("Evt") })

        // No handler running — bookmark stays at 0
        await expect(
            waitUntilProcessed(pool, HANDLER, SequencePosition.fromString("1"), { timeoutMs: 200 })
        ).rejects.toThrow(WaitTimeoutError)
    })

    test("works with concurrent waiters on different positions", async () => {
        const controller = new AbortController()

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER,
            handlerFactory: () => ({
                when: {
                    Evt: async () => {}
                }
            }),
            signal: controller.signal
        })

        const pos1 = await store.append({ events: event("Evt") })
        const pos2 = await store.append({ events: event("Evt") })

        // Both should resolve
        await Promise.all([waitUntilProcessed(pool, HANDLER, pos1), waitUntilProcessed(pool, HANDLER, pos2)])

        controller.abort()
        await promise.catch(() => {})
    })
})
