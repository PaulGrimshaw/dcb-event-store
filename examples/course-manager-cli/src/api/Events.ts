import { Tags } from "@dcb-es/event-store"
import { DcbEvent } from "@dcb-es/event-store"

export class CourseWasRegisteredEvent implements DcbEvent {
    public type: "courseWasRegistered" = "courseWasRegistered"
    public tags: Tags
    public data: { courseId: string; title: string; capacity: number }
    public metadata: unknown = {}

    constructor({ courseId, title, capacity }: { courseId: string; title: string; capacity: number }) {
        this.tags = Tags.fromObj({ courseId })
        this.data = { title, capacity, courseId }
    }
}

export class StudentWasRegistered implements DcbEvent {
    public type: "studentWasRegistered" = "studentWasRegistered"
    public tags: Tags
    public data: { studentId: string; name: string; studentNumber: number }
    public metadata: unknown = {}

    constructor({ studentId, name, studentNumber }: { studentId: string; name: string; studentNumber: number }) {
        // studentNumberIndex tag enables scoped locking for the NextStudentNumber decision model.
        // Without it, the global student number query has no tag to lock on.
        this.tags = Tags.fromObj({ studentId, studentNumberIndex: "global" })
        this.data = { studentId, name, studentNumber }
    }
}

export class CourseCapacityWasChangedEvent implements DcbEvent {
    type: "courseCapacityWasChanged" = "courseCapacityWasChanged"
    public tags: Tags
    public data: { courseId: string; newCapacity: number }
    public metadata: unknown = {}

    constructor({ courseId, newCapacity }: { courseId: string; newCapacity: number }) {
        this.tags = Tags.fromObj({ courseId })
        this.data = { courseId, newCapacity }
    }
}

export class CourseTitleWasChangedEvent implements DcbEvent {
    type: "courseTitleWasChanged" = "courseTitleWasChanged"
    public tags: Tags
    public data: { courseId: string; newTitle: string }
    public metadata: unknown = {}

    constructor({ courseId, newTitle }: { courseId: string; newTitle: string }) {
        this.tags = Tags.fromObj({ courseId })
        this.data = { courseId, newTitle }
    }
}

export class StudentWasSubscribedEvent implements DcbEvent {
    type: "studentWasSubscribed" = "studentWasSubscribed"
    public tags: Tags
    public data: { courseId: string; studentId: string }
    public metadata: unknown = {}

    constructor({ studentId, courseId }: { studentId: string; courseId: string }) {
        this.tags = Tags.fromObj({ studentId, courseId })
        this.data = { studentId, courseId }
    }
}

export class StudentWasUnsubscribedEvent implements DcbEvent {
    type: "studentWasUnsubscribed" = "studentWasUnsubscribed"
    public tags: Tags
    public data: { courseId: string; studentId: string }
    public metadata: unknown = {}

    constructor({ studentId, courseId }: { studentId: string; courseId: string }) {
        this.tags = Tags.fromObj({ studentId, courseId })
        this.data = { studentId, courseId }
    }
}
