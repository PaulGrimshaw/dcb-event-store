import { Pool } from "pg"
import { Api } from "./Api.js"
import { getTestPgDatabasePool } from "@test/testPgDbPool"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"

const COURSE_1 = {
    id: "course-1",
    title: "Course 1",
    capacity: 5
}

describe("EventSourcedApi", () => {
    let pool: Pool
    let api: Api

    beforeAll(async () => {
        pool = await getTestPgDatabasePool()
        const eventStore = new PostgresEventStore({ pool })
        await eventStore.ensureInstalled()
        api = new Api(eventStore)
    })

    afterEach(async () => {
        await pool.query("DELETE FROM events")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    describe("with one course and 100 students in database", () => {
        beforeEach(async () => {
            await api.registerCourse({ id: COURSE_1.id, title: COURSE_1.title, capacity: COURSE_1.capacity })

            for (let i = 0; i < 100; i++) {
                await api.registerStudent({ id: `student-${i}`, name: `Student ${i}` })
            }
        })

        test("should throw error when 6th student subscribes", async () => {
            for (let i = 1; i <= 5; i++) {
                await api.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: `student-${i}` })
            }

            await expect(
                api.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: "student-6" })
            ).rejects.toThrow(`Course ${COURSE_1.id} is full.`)
        })

        test("should reject subscriptions when 10 students subscribe simultaneously", async () => {
            const studentSubscriptionPromises: Promise<void>[] = []

            for (let i = 0; i < 10; i++) {
                studentSubscriptionPromises.push(
                    api.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: `student-${i}` })
                )
            }

            const results = await Promise.allSettled(studentSubscriptionPromises)
            const succeeded = results.filter(result => result.status === "fulfilled").length
            expect(succeeded).toBeLessThanOrEqual(COURSE_1.capacity)
        })
    })
})
