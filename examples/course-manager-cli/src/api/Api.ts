import { buildDecisionModel } from "@dcb-es/event-store"
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
import { EventStore } from "@dcb-es/event-store"

const STUDENT_SUBSCRIPTION_LIMIT = 5

export class Api {
    constructor(private eventStore: EventStore) {}

    async registerCourse(cmd: { id: string; title: string; capacity: number }) {
        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(cmd.id)
        })

        if (state.courseExists) throw new Error(`Course with id ${cmd.id} already exists`)

        await this.eventStore.append({
            events: new CourseWasRegisteredEvent({ courseId: cmd.id, title: cmd.title, capacity: cmd.capacity }),
            condition: appendCondition
        })
    }

    async registerStudent(cmd: { id: string; name: string }) {
        const { id, name } = cmd
        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            studentAlreadyRegistered: StudentAlreadyRegistered(id),
            nextStudentNumber: NextStudentNumber()
        })

        if (state.studentAlreadyRegistered) throw new Error(`Student with id ${id} already registered.`)

        await this.eventStore.append({
            events: new StudentWasRegistered({ studentId: id, name, studentNumber: state.nextStudentNumber }),
            condition: appendCondition
        })
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

        await this.eventStore.append({
            events: new CourseCapacityWasChangedEvent({ courseId, newCapacity }),
            condition: appendCondition
        })
    }

    async updateCourseTitle(cmd: { courseId: string; newTitle: string }) {
        const { courseId, newTitle } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(courseId),
            courseTitle: CourseTitle(courseId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (state.courseTitle === newTitle) throw new Error("New title is the same as the current title.")

        await this.eventStore.append({
            events: new CourseTitleWasChangedEvent({ courseId, newTitle }),
            condition: appendCondition
        })
    }

    async subscribeStudentToCourse(cmd: { courseId: string; studentId: string }) {
        const { courseId, studentId } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            courseExists: CourseExists(courseId),
            courseCapacity: CourseCapacity(courseId),
            studentAlreadySubscribed: StudentAlreadySubscribed({
                courseId: courseId,
                studentId: studentId
            }),
            studentSubscriptions: StudentSubscriptions(studentId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)

        if (state.courseCapacity.subscriberCount >= state.courseCapacity.capacity)
            throw new Error(`Course ${courseId} is full.`)

        if (state.studentAlreadySubscribed)
            throw new Error(`Student ${studentId} already subscribed to course ${courseId}.`)

        if (state.studentSubscriptions.subscriptionCount >= STUDENT_SUBSCRIPTION_LIMIT)
            throw new Error(`Student ${studentId} is already subscribed to the maximum number of courses`)

        await this.eventStore.append({
            events: new StudentWasSubscribedEvent({ courseId, studentId }),
            condition: appendCondition
        })
    }

    async unsubscribeStudentFromCourse(cmd: { courseId: string; studentId: string }) {
        const { courseId, studentId } = cmd

        const { state, appendCondition } = await buildDecisionModel(this.eventStore, {
            studentAlreadySubscribed: StudentAlreadySubscribed({
                courseId: courseId,
                studentId: studentId
            }),
            courseExists: CourseExists(courseId)
        })

        if (!state.courseExists) throw new Error(`Course ${courseId} doesn't exist.`)
        if (!state.studentAlreadySubscribed)
            throw new Error(`Student ${studentId} is not subscribed to course ${courseId}.`)

        await this.eventStore.append({
            events: new StudentWasUnsubscribedEvent({ courseId, studentId }),
            condition: appendCondition
        })
    }
}
