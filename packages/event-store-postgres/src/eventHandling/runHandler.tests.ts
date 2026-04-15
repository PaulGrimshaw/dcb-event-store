import { Pool } from "pg"
import { DcbEvent, Tags } from "@dcb-es/event-store"
import { PostgresEventStore } from "../eventStore/PostgresEventStore.js"
import { runHandler } from "./runHandler.js"
import { ensureHandlersInstalled } from "./ensureHandlersInstalled.js"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const event = (type: string, tags: Tags = Tags.fromObj({ e: "1" }), data: unknown = {}): DcbEvent => ({
    type,
    tags,
    data,
    metadata: {}
})

describe("runHandler", () => {
    let pool: Pool
    let store: PostgresEventStore
    const HANDLER_NAME = "TestProjection"

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 20 })
        store = new PostgresEventStore({ pool })
        await store.ensureInstalled()
        await ensureHandlersInstalled(pool, [HANDLER_NAME], "_handler_bookmarks")
    })

    afterEach(async () => {
        await pool.query("TRUNCATE table events")
        await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
        await pool.query("UPDATE _handler_bookmarks SET last_sequence_position = 0")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    test("processes historical events and advances bookmark", async () => {
        await store.append({ events: event("A") })
        await store.append({ events: event("B") })

        const processed: string[] = []
        const controller = new AbortController()

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    A: async ({ event: { type } }) => {
                        processed.push(type)
                    },
                    B: async ({ event: { type } }) => {
                        processed.push(type)
                        controller.abort()
                    }
                }
            }),
            signal: controller.signal
        })

        await promise
        expect(processed).toEqual(["A", "B"])

        const bookmark = await pool.query(
            "SELECT last_sequence_position FROM _handler_bookmarks WHERE handler_id = $1",
            [HANDLER_NAME]
        )
        expect(Number(bookmark.rows[0].last_sequence_position)).toBe(2)
    })

    test("processes live events as they arrive", async () => {
        const processed: string[] = []
        const controller = new AbortController()

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    Live: async ({ event: { type } }) => {
                        processed.push(type)
                        if (processed.length >= 2) controller.abort()
                    }
                }
            }),
            signal: controller.signal
        })

        await store.append({ events: event("Live") })
        await store.append({ events: event("Live") })

        await promise
        expect(processed).toEqual(["Live", "Live"])
    })

    test("bookmark + projection update are atomic", async () => {
        let callCount = 0
        const controller = new AbortController()

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: client => ({
                when: {
                    Counter: async () => {
                        callCount++
                        await client.query("CREATE TABLE IF NOT EXISTS _test_counter (n INT)")
                        await client.query("INSERT INTO _test_counter VALUES ($1)", [callCount])
                        controller.abort()
                    }
                }
            }),
            signal: controller.signal
        })

        await store.append({ events: event("Counter") })
        await promise

        const counter = await pool.query("SELECT COUNT(*) as n FROM _test_counter")
        expect(Number(counter.rows[0].n)).toBe(1)

        const bookmark = await pool.query(
            "SELECT last_sequence_position FROM _handler_bookmarks WHERE handler_id = $1",
            [HANDLER_NAME]
        )
        expect(Number(bookmark.rows[0].last_sequence_position)).toBe(1)

        await pool.query("DROP TABLE IF EXISTS _test_counter")
    })

    test("fires NOTIFY on bookmark advance", async () => {
        const controller = new AbortController()
        const notifications: string[] = []

        const listener = await pool.connect()
        await listener.query("LISTEN _handler_bookmarks")
        listener.on("notification", msg => {
            if (msg.payload) notifications.push(msg.payload)
        })

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    Notify: async () => {
                        controller.abort()
                    }
                }
            }),
            signal: controller.signal
        })

        await store.append({ events: event("Notify") })
        await promise

        // Give notification time to arrive
        await new Promise(r => setTimeout(r, 50))

        await listener.query("UNLISTEN _handler_bookmarks")
        listener.release()

        expect(notifications.length).toBe(1)
        expect(notifications[0]).toBe(`${HANDLER_NAME}:1`)
    })

    test("handler restart resumes from bookmark (no reprocessing of committed events)", async () => {
        await store.append({ events: event("First") })

        // First run: process one event then abort
        const controller1 = new AbortController()

        const { promise: p1 } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    First: async () => {
                        controller1.abort()
                    }
                }
            }),
            signal: controller1.signal
        })
        await p1

        // Bookmark should be at 1
        const bm1 = await pool.query("SELECT last_sequence_position FROM _handler_bookmarks WHERE handler_id = $1", [
            HANDLER_NAME
        ])
        expect(Number(bm1.rows[0].last_sequence_position)).toBe(1)

        // Append another event
        await store.append({ events: event("Second") })

        // Second run: should resume from bookmark (position 1), only see "Second"
        const controller2 = new AbortController()
        const run2Processed: string[] = []

        const { promise: p2 } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    First: async () => {
                        run2Processed.push("First")
                    },
                    Second: async () => {
                        run2Processed.push("Second")
                        controller2.abort()
                    }
                }
            }),
            signal: controller2.signal
        })
        await p2
        expect(run2Processed).toEqual(["Second"])
    })

    test("handler error propagates via promise rejection", async () => {
        await store.append({ events: event("Boom") })

        const { promise } = runHandler({
            pool,
            eventStore: store,
            handlerName: HANDLER_NAME,
            handlerFactory: () => ({
                when: {
                    Boom: async () => {
                        throw new Error("handler exploded")
                    }
                }
            })
        })

        await expect(promise).rejects.toThrow("handler exploded")

        // Bookmark should NOT have advanced (transaction rolled back)
        const bookmark = await pool.query(
            "SELECT last_sequence_position FROM _handler_bookmarks WHERE handler_id = $1",
            [HANDLER_NAME]
        )
        expect(Number(bookmark.rows[0].last_sequence_position)).toBe(0)
    })

    test("rejects handler names containing colon", () => {
        expect(() =>
            runHandler({
                pool,
                eventStore: store,
                handlerName: "bad:name",
                handlerFactory: () => ({ when: {} })
            })
        ).toThrow('must not contain ":"')
    })
})
