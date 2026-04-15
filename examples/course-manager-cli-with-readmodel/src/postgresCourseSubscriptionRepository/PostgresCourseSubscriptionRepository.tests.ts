import { Pool, PoolClient } from "pg"
import {
    installPostgresCourseSubscriptionsRepository,
    PostgresCourseSubscriptionsRepository,
    Student
} from "./PostgresCourseSubscriptionRespository.js"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const COURSE_1 = {
    id: "course-1",
    title: "Course 1",
    capacity: 30
}
const STUDENT_1 = {
    id: "student-1",
    name: "John Doe",
    studentNumber: 1
}

describe("PostgresCourseSubscriptionRepository", () => {
    let pool: Pool
    let client: PoolClient
    let repository: ReturnType<typeof PostgresCourseSubscriptionsRepository>

    beforeAll(async () => {
        pool = await getTestPgDatabasePool()
        const installClient = await pool.connect()
        await installClient.query("BEGIN")
        await installPostgresCourseSubscriptionsRepository(installClient)
        await installClient.query("COMMIT")
        installClient.release()
    })

    beforeEach(async () => {
        client = await pool.connect()
        await client.query("BEGIN")
        repository = PostgresCourseSubscriptionsRepository(client)
    })

    afterEach(async () => {
        await client.query("ROLLBACK")
        client.release()
        await pool.query("TRUNCATE table courses")
        await pool.query("TRUNCATE table students")
        await pool.query("TRUNCATE table subscriptions")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    describe("registerCourse", () => {
        test("should register a course successfully", async () => {
            await repository.registerCourse({
                courseId: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity
            })
            const course = await repository.findCourseById(COURSE_1.id)
            expect(course).toEqual({
                id: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity,
                subscribedStudents: []
            })
        })
    })

    describe("findCourseById", () => {
        test("should return a course by ID with correct capacity and empty subscriptions", async () => {
            await repository.registerCourse({
                courseId: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity
            })
            const course = await repository.findCourseById(COURSE_1.id)

            expect(course).toEqual({
                id: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity,
                subscribedStudents: []
            })
        })
        test("should return undefined if the course does not exist", async () => {
            const course = await repository.findCourseById("non-existent-course")
            expect(course).toBeUndefined()
        })
    })

    describe("registerStudent", () => {
        test("should register student successfully", async () => {
            await repository.registerStudent({
                studentId: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber
            })
            expect(await repository.findStudentById(STUDENT_1.id)).toEqual({
                id: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber,
                subscribedCourses: []
            })
        })
    })

    describe("with one existing course and one registered student", () => {
        beforeEach(async () => {
            await repository.registerCourse({
                courseId: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity
            })
            await repository.registerStudent({
                studentId: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber
            })
        })

        describe("updateCourseCapacity", () => {
            const newCapacity = 40

            test("should update a course's capacity successfully", async () => {
                await repository.updateCourseCapacity({ courseId: COURSE_1.id, newCapacity })
                const course = await repository.findCourseById(COURSE_1.id)
                expect(course?.capacity).toBe(newCapacity)
            })
        })

        describe("subscribeStudentToCourse", () => {
            test("should subscribe a student to a course successfully", async () => {
                await repository.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: STUDENT_1.id })
                const course = await repository.findCourseById(COURSE_1.id)
                const student = await repository.findStudentById(STUDENT_1.id)

                expect(course?.subscribedStudents).toEqual([
                    { id: STUDENT_1.id, name: STUDENT_1.name, studentNumber: STUDENT_1.studentNumber }
                ])
                expect(student?.subscribedCourses).toEqual([
                    { id: COURSE_1.id, title: COURSE_1.title, capacity: COURSE_1.capacity }
                ])
            })
        })

        describe("unsubscribeStudentFromCourse", () => {
            test("should unsubscribe a student from a course successfully", async () => {
                await repository.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: STUDENT_1.id })
                await repository.unsubscribeStudentFromCourse({ courseId: COURSE_1.id, studentId: STUDENT_1.id })
                const course = await repository.findCourseById(COURSE_1.id)
                const student = await repository.findStudentById(STUDENT_1.id)

                expect(course?.subscribedStudents).toEqual([])
                expect(student?.subscribedCourses).toEqual([])
            })
        })
    })

    describe("findStudentById", () => {
        test("should return a student by ID with correct name and no subscribed courses", async () => {
            await repository.registerStudent({
                studentId: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber
            })
            const student = await repository.findStudentById(STUDENT_1.id)

            expect(student).toEqual(<Student>{
                id: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber,
                subscribedCourses: []
            })
        })

        test("should return a student by ID with subscribed courses", async () => {
            await repository.registerStudent({
                studentId: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber
            })
            await repository.registerCourse({
                courseId: COURSE_1.id,
                title: COURSE_1.title,
                capacity: COURSE_1.capacity
            })
            await repository.subscribeStudentToCourse({ courseId: COURSE_1.id, studentId: STUDENT_1.id })

            const student = await repository.findStudentById(STUDENT_1.id)

            expect(student).toEqual({
                id: STUDENT_1.id,
                name: STUDENT_1.name,
                studentNumber: STUDENT_1.studentNumber,
                subscribedCourses: [
                    {
                        id: COURSE_1.id,
                        title: COURSE_1.title,
                        capacity: COURSE_1.capacity
                    }
                ]
            })
        })

        test("should return undefined if the student does not exist", async () => {
            const student = await repository.findStudentById("non-existent-student")
            expect(student).toBeUndefined()
        })
    })
})
