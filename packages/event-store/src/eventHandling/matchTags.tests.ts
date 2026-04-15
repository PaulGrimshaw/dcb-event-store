import { Tags } from "../eventStore/Tags.js"
import { matchTags } from "./matchTags.js"

describe("matchTags", () => {
    test("should return true when no tagFilter", () => {
        const tags: Tags = Tags.fromObj({ courseId: "1" })
        const tagFilter: Tags = Tags.createEmpty()

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(true)
    })

    test("should return false when tagFilter has different tag value", () => {
        const tags: Tags = Tags.fromObj({ courseId: "c1" })
        const tagFilter: Tags = Tags.fromObj({ courseId: "c2" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(false)
    })

    test("should return false when tags are empty", () => {
        const tags: Tags = Tags.createEmpty()
        const tagFilter: Tags = Tags.fromObj({ courseId: "c2" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(false)
    })

    test("should return false when tags are different keys", () => {
        const tags: Tags = Tags.fromObj({ studentId: "s1" })
        const tagFilter: Tags = Tags.fromObj({ courseId: "c2" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(false)
    })

    test("should return true when some tags are matched", () => {
        const tags: Tags = Tags.fromObj({ studentId: "s1", courseId: "c1" })
        const tagFilter: Tags = Tags.fromObj({ courseId: "c1" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(true)
    })

    test("should return false when some tags are matched but filter still has more", () => {
        const tags: Tags = Tags.fromObj({ studentId: "s1" })
        const tagFilter: Tags = Tags.fromObj({ courseId: "c1", studentId: "s1" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(false)
    })

    test("should return false when double tags are matched", () => {
        const tags: Tags = Tags.fromObj({ courseId: "c1", studentId: "s1" })
        const tagFilter: Tags = Tags.fromObj({ courseId: "c1", studentId: "s1" })

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(true)
    })

    test("should return true when undefinted tagFilter", () => {
        const tags: Tags = Tags.fromObj({ courseId: "c1", studentId: "s1" })

        const result = matchTags({ tags, tagFilter: undefined })
        expect(result).toBe(true)
    })

    test("should return true when non-key-value tags match", () => {
        const tags = Tags.from(["active", "courseId=c1"])
        const tagFilter = Tags.from(["active"])

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(true)
    })

    test("should return false when non-key-value tags do not match", () => {
        const tags = Tags.from(["inactive", "courseId=c1"])
        const tagFilter = Tags.from(["active"])

        const result = matchTags({ tags, tagFilter })
        expect(result).toBe(false)
    })
})
