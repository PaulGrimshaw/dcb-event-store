import { Tags } from "../eventStore/Tags.js"
import { EventHandlerWithState } from "./EventHandlerWithState.js"
import {
    CourseWasRegisteredEvent,
    CourseCapacityWasChangedEvent,
    StudentWasSubscribedEvent,
    StudentWasUnsubscribedEvent
} from "./buildDecisionModel.tests.events.js"

export const CourseExists = (courseId: string): EventHandlerWithState<CourseWasRegisteredEvent, boolean> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: false,
    when: {
        courseWasRegistered: async () => true
    }
})

export const CourseCapacity = (
    courseId: string
): EventHandlerWithState<
    CourseWasRegisteredEvent | CourseCapacityWasChangedEvent | StudentWasSubscribedEvent | StudentWasUnsubscribedEvent,
    { isFull: boolean; subscriberCount: number; capacity: number }
> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: { isFull: true, subscriberCount: 0, capacity: 0 },
    when: {
        courseWasRegistered: ({ event }) => ({
            isFull: event.data.capacity === 0,
            capacity: event.data.capacity,
            subscriberCount: 0
        }),
        courseCapacityWasChanged: ({ event }, { subscriberCount }) => ({
            subscriberCount,
            isFull: event.data.newCapacity <= subscriberCount,
            capacity: event.data.newCapacity
        }),
        studentWasSubscribed: (_eventEnvelope, { capacity, subscriberCount }) => ({
            isFull: capacity <= subscriberCount + 1,
            subscriberCount: subscriberCount + 1,
            capacity
        }),
        studentWasUnsubscribed: (eventEnvelope, { capacity, subscriberCount }) => ({
            isFull: capacity <= subscriberCount - 1,
            subscriberCount: subscriberCount - 1,
            capacity
        })
    }
})
