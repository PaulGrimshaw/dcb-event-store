import { buildDecisionModel, EventStore, SequencePosition } from "@dcb-es/event-store"
import {
    PostgresCourseSubscriptionsRepository,
    STUDENT_SUBSCRIPTION_LIMIT
} from "../postgresCourseSubscriptionRepository/PostgresCourseSubscriptionRespository"
import { Pool } from "pg"
import {
    CourseWasRegisteredEvent,
    StudentWasRegistered,
    StudentWasSubscribedEvent,
    StudentWasUnsubscribedEvent,
    CourseCapacityWasChangedEvent,
    CourseTitleWasChangedEvent
} from "./Events"

import {
    CourseCapacity,
    CourseExists,
    CourseTitle,
    NextStudentNumber,
    StudentAlreadyRegistered,
    StudentAlreadySubscribed,
    StudentSubscriptions
} from "./DecisionModels"
import { waitUntilProcessed } from "@dcb-es/event-store-postgres"

export const PROJECTION_NAME = "CourseProjection"

export class Api {
    private readModelRepository: ReturnType<typeof PostgresCourseSubscriptionsRepository>

    constructor(
        private pool: Pool,
        private eventStore: EventStore
    ) {
        this.readModelRepository = PostgresCourseSubscriptionsRepository(pool)
    }

    async findCourseById(courseId: string) {
        return this.readModelRepository.findCourseById(courseId)
    }

    async findStudentById(studentId: string) {
        return this.readModelRepository.findStudentById(studentId)
    }

    async registerCourse(cmd: { id: string; title: string; capacity: number }) {
        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(cmd.id)
        })

        if (state.courseExists) throw new Error(`Course with id ${cmd.id} already exists`)

        const position = await this.eventStore.append({
            events: new CourseWasRegisteredEvent({ courseId: cmd.id, title: cmd.title, capacity: cmd.capacity }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    async registerStudent(cmd: { id: string; name: string }) {
        const { id, name } = cmd
        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            studentAlreadyRegistered: StudentAlreadyRegistered(id),
            nextStudentNumber: NextStudentNumber()
        })

        if (state.studentAlreadyRegistered) throw new Error(`Student with id ${id} already registered.`)

        const position = await this.eventStore.append({
            events: new StudentWasRegistered({ studentId: id, name, studentNumber: state.nextStudentNumber }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    async updateCourseCapacity(cmd: { courseId: string; newCapacity: number }) {
        const { courseId, newCapacity } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(courseId),
            CourseCapacity: CourseCapacity(courseId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (state.CourseCapacity.capacity === newCapacity)
            throw new Error("New capacity is the same as the current capacity.")

        const position = await this.eventStore.append({
            events: new CourseCapacityWasChangedEvent({ courseId, newCapacity }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    async updateCourseTitle(cmd: { courseId: string; newTitle: string }) {
        const { courseId, newTitle } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(courseId),
            courseTitle: CourseTitle(courseId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (state.courseTitle === newTitle) throw new Error("New title is the same as the current title.")

        const position = await this.eventStore.append({
            events: new CourseTitleWasChangedEvent({ courseId, newTitle }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    async subscribeStudentToCourse(cmd: { courseId: string; studentId: string }) {
        const { courseId, studentId } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(courseId),
            courseCapacity: CourseCapacity(courseId),
            studentAlreadySubscribed: StudentAlreadySubscribed({ courseId, studentId }),
            studentSubscriptions: StudentSubscriptions(studentId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (state.courseCapacity.subscriberCount >= state.courseCapacity.capacity)
            throw new Error(`Course ${courseId} is full.`)
        if (state.studentAlreadySubscribed)
            throw new Error(`Student ${studentId} already subscribed to course ${courseId}.`)
        if (state.studentSubscriptions.subscriptionCount >= STUDENT_SUBSCRIPTION_LIMIT)
            throw new Error(`Student ${studentId} is already subscribed to the maximum number of courses`)

        const position = await this.eventStore.append({
            events: new StudentWasSubscribedEvent({ courseId, studentId }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    async unsubscribeStudentFromCourse(cmd: { courseId: string; studentId: string }) {
        const { courseId, studentId } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            studentAlreadySubscribed: StudentAlreadySubscribed({ courseId, studentId }),
            courseExists: CourseExists(courseId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (!state.studentAlreadySubscribed)
            throw new Error(`Student ${studentId} is not subscribed to course ${courseId}.`)

        const position = await this.eventStore.append({
            events: new StudentWasUnsubscribedEvent({ courseId, studentId }),
            condition: appendCondition
        })
        await this.waitForProjection(position)
    }

    private waitForProjection(position: SequencePosition): Promise<void> {
        return waitUntilProcessed(this.pool, PROJECTION_NAME, position)
    }
}
