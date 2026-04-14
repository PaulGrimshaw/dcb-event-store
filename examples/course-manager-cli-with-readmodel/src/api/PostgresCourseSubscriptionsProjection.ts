import {
    CourseWasRegisteredEvent,
    CourseCapacityWasChangedEvent,
    CourseTitleWasChangedEvent,
    StudentWasRegistered,
    StudentWasSubscribedEvent,
    StudentWasUnsubscribedEvent
} from "./Events"
import { PostgresCourseSubscriptionsRepository } from "../postgresCourseSubscriptionRepository/PostgresCourseSubscriptionRespository"
import { Pool } from "pg"
import { EventHandler } from "@dcb-es/event-store"

export const PostgresCourseSubscriptionsProjection = (
    pool: Pool
): EventHandler<
    | CourseWasRegisteredEvent
    | CourseCapacityWasChangedEvent
    | CourseTitleWasChangedEvent
    | StudentWasRegistered
    | StudentWasSubscribedEvent
    | StudentWasUnsubscribedEvent
> => ({
    when: {
        courseWasRegistered: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.registerCourse({ courseId: data.courseId, title: data.title, capacity: data.capacity })
        },
        courseTitleWasChanged: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.updateCourseTitle({ courseId: data.courseId, newTitle: data.newTitle })
        },
        courseCapacityWasChanged: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.updateCourseCapacity({ courseId: data.courseId, newCapacity: data.newCapacity })
        },
        studentWasRegistered: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.registerStudent({
                studentId: data.studentId,
                name: data.name,
                studentNumber: data.studentNumber
            })
        },
        studentWasSubscribed: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.subscribeStudentToCourse({ studentId: data.studentId, courseId: data.courseId })
        },
        studentWasUnsubscribed: async ({ event: { data } }) => {
            const repository = PostgresCourseSubscriptionsRepository(pool)
            await repository.unsubscribeStudentFromCourse({ studentId: data.studentId, courseId: data.courseId })
        }
    }
})
