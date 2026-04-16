import { Pool, Client } from "pg"
import { v4 as uuid } from "uuid"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"
import { EventStore } from "@dcb-es/event-store"
import { EventStoreFactory, ThresholdFactory } from "./harness/types"

export function createPgFactory(pool: Pool, isolated = false): EventStoreFactory {
    return async (runId: string): Promise<EventStore> => {
        const tablePrefix = isolated ? runId.replace(/[^a-z0-9_]/gi, "_").slice(0, 40) : undefined
        const store = new PostgresEventStore({ pool, tablePrefix })
        await store.ensureInstalled()
        return store
    }
}

export function createThresholdFactory(pool: Pool): ThresholdFactory {
    return async (_runId: string, copyThreshold: number): Promise<EventStore> => {
        const store = new PostgresEventStore({ pool, copyThreshold })
        await store.ensureInstalled()
        return store
    }
}

export function createBenchPool(connectionString: string, max: number): Pool {
    const pool = new Pool({ connectionString, max })
    pool.on("connect", (client: Client) => {
        client.query("SET synchronous_commit = off")
    })
    return pool
}

export async function createTestDb(baseUri: string): Promise<{ connectionString: string; dbName: string }> {
    const dbName = `bench_${uuid().split("-").join("").slice(0, 16)}`
    const client = new Client({ connectionString: baseUri })
    await client.connect()
    await client.query(`CREATE DATABASE "${dbName}"`)
    await client.end()

    const url = new URL(baseUri)
    url.pathname = `/${dbName}`
    return { connectionString: url.toString(), dbName }
}

export async function dropTestDb(baseUri: string, dbName: string): Promise<void> {
    const client = new Client({ connectionString: baseUri })
    await client.connect()
    await client.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`)
    await client.end()
}

export async function pgAvailable(baseUri: string): Promise<boolean> {
    const client = new Client({ connectionString: baseUri })
    try {
        await client.connect()
        await client.end()
        return true
    } catch {
        return false
    }
}
