import { EventHandlerWithState, Tags } from "@dcb-es/event-store"
import {
    CourseWasRegisteredEvent,
    CourseCapacityWasChangedEvent,
    StudentWasSubscribedEvent,
    StudentWasUnsubscribedEvent,
    StudentWasRegistered,
    CourseTitleWasChangedEvent
} from "./Events"

export const CourseExists = (courseId: string): EventHandlerWithState<CourseWasRegisteredEvent, boolean> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: false,
    when: {
        courseWasRegistered: () => true
    }
})

export const CourseTitle = (
    courseId: string
): EventHandlerWithState<CourseWasRegisteredEvent | CourseTitleWasChangedEvent, string> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: "",
    when: {
        courseWasRegistered: ({ event }) => event.data.title,
        courseTitleWasChanged: ({ event }) => event.data.newTitle
    }
})

export const CourseCapacity = (
    courseId: string
): EventHandlerWithState<
    CourseWasRegisteredEvent | CourseCapacityWasChangedEvent | StudentWasSubscribedEvent | StudentWasUnsubscribedEvent,
    { subscriberCount: number; capacity: number }
> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: { subscriberCount: 0, capacity: 0 },
    when: {
        courseWasRegistered: ({ event }) => ({
            capacity: event.data.capacity,
            subscriberCount: 0
        }),
        courseCapacityWasChanged: ({ event }, { subscriberCount }) => ({
            subscriberCount,
            capacity: event.data.newCapacity
        }),
        studentWasSubscribed: (_eventEnvelope, { capacity, subscriberCount }) => ({
            subscriberCount: subscriberCount + 1,
            capacity
        }),
        studentWasUnsubscribed: (eventEnvelope, { capacity, subscriberCount }) => ({
            subscriberCount: subscriberCount - 1,
            capacity
        })
    }
})

export const StudentAlreadyRegistered = (studentId: string): EventHandlerWithState<StudentWasRegistered, boolean> => ({
    tagFilter: Tags.fromObj({ studentId }),
    init: false,
    when: {
        studentWasRegistered: () => true
    }
})

export const NextStudentNumber = (): EventHandlerWithState<StudentWasRegistered, number> => ({
    tagFilter: Tags.fromObj({ studentNumberIndex: "global" }),
    init: 1,
    onlyLastEvent: true,
    when: {
        studentWasRegistered: ({ event }) => event.data.studentNumber + 1
    }
})

export const StudentAlreadySubscribed = ({
    courseId,
    studentId
}: {
    courseId: string
    studentId: string
}): EventHandlerWithState<StudentWasSubscribedEvent | StudentWasUnsubscribedEvent, boolean> => ({
    tagFilter: Tags.fromObj({ courseId, studentId }),
    init: false,
    when: {
        studentWasSubscribed: () => true,
        studentWasUnsubscribed: () => false
    }
})

export const StudentSubscriptions = (
    studentId: string
): EventHandlerWithState<StudentWasSubscribedEvent | StudentWasUnsubscribedEvent, { subscriptionCount: number }> => ({
    tagFilter: Tags.fromObj({ studentId }),
    init: { subscriptionCount: 0 },
    when: {
        studentWasSubscribed: (_eventEnvelope, { subscriptionCount }) => ({
            subscriptionCount: subscriptionCount + 1
        }),
        studentWasUnsubscribed: (_eventEnvelope, { subscriptionCount }) => ({
            subscriptionCount: subscriptionCount - 1
        })
    }
})
