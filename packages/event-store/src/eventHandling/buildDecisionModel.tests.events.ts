import { DcbEvent } from "../eventStore/EventStore.js"
import { Tags } from "../eventStore/Tags.js"

export class CourseWasRegisteredEvent implements DcbEvent {
    public type: "courseWasRegistered" = "courseWasRegistered"
    public tags: Tags
    public data: { capacity: number }
    public metadata: Record<string, never> = {}

    constructor({ courseId, capacity }: { courseId: string; capacity: number }) {
        this.tags = Tags.fromObj({ courseId })
        this.data = { capacity }
    }
}

export class CourseCapacityWasChangedEvent implements DcbEvent {
    public type: "courseCapacityWasChanged" = "courseCapacityWasChanged"
    public tags: Tags
    public data: { newCapacity: number }
    public metadata: Record<string, never> = {}

    constructor({ courseId, newCapacity }: { courseId: string; newCapacity: number }) {
        this.tags = Tags.fromObj({ courseId })
        this.data = { newCapacity }
    }
}

export class StudentWasSubscribedEvent implements DcbEvent {
    public type: "studentWasSubscribed" = "studentWasSubscribed"
    public tags: Tags
    public data: Record<string, never> = {}
    public metadata: Record<string, never> = {}

    constructor({ studentId, courseId }: { studentId: string; courseId: string }) {
        this.tags = Tags.fromObj({ studentId, courseId })
    }
}

export class StudentWasUnsubscribedEvent implements DcbEvent {
    public type: "studentWasUnsubscribed" = "studentWasUnsubscribed"
    public tags: Tags
    public data: Record<string, never> = {}
    public metadata: Record<string, never> = {}

    constructor({ studentId, courseId }: { studentId: string; courseId: string }) {
        this.tags = Tags.fromObj({ studentId, courseId })
    }
}
