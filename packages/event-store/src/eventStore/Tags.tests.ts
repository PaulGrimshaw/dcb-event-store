import { Tags } from "./Tags"

describe("Tags", () => {
    describe("from()", () => {
        test("should create tags from key=value strings", () => {
            const tags = Tags.from(["courseId=c1", "studentId=s1"])
            expect(tags.values).toEqual(["courseId=c1", "studentId=s1"])
        })

        test("should allow tags without key=value format", () => {
            const tags = Tags.from(["my-tag", "another_tag"])
            expect(tags.values).toEqual(["my-tag", "another_tag"])
        })

        test("should allow single-character tags", () => {
            const tags = Tags.from(["x"])
            expect(tags.values).toEqual(["x"])
        })

        test("should allow tags with hyphens", () => {
            const tags = Tags.from(["course-id=c1", "student-id=s1"])
            expect(tags.values).toEqual(["course-id=c1", "student-id=s1"])
        })

        test("should allow tags with underscores", () => {
            const tags = Tags.from(["course_id"])
            expect(tags.values).toEqual(["course_id"])
        })

        test("should allow tags with colons", () => {
            const tags = Tags.from(["course:123"])
            expect(tags.values).toEqual(["course:123"])
        })

        test("should allow tags with slashes", () => {
            const tags = Tags.from(["user/admin"])
            expect(tags.values).toEqual(["user/admin"])
        })

        test("should allow tags with dots", () => {
            const tags = Tags.from(["com.example.tag"])
            expect(tags.values).toEqual(["com.example.tag"])
        })

        test("should allow tags with multiple equals signs", () => {
            const tags = Tags.from(["key==value"])
            expect(tags.values).toEqual(["key==value"])
        })

        test("should allow tags with special characters", () => {
            const tags = Tags.from(["key=value!", "@tag", "#hashtag", "price=$100"])
            expect(tags.values).toEqual(["key=value!", "@tag", "#hashtag", "price=$100"])
        })

        test("should allow mixed key=value and non-key=value tags", () => {
            const tags = Tags.from(["courseId=c1", "active", "priority:high"])
            expect(tags.values).toEqual(["courseId=c1", "active", "priority:high"])
        })

        test("should allow numeric-only tags", () => {
            const tags = Tags.from(["12345"])
            expect(tags.values).toEqual(["12345"])
        })

        test("should create tags from an empty array", () => {
            const tags = Tags.from([])
            expect(tags.values).toEqual([])
            expect(tags.length).toBe(0)
        })

        test("should throw error for empty string tag", () => {
            expect(() => Tags.from([""])).toThrow(/Invalid tag value/)
        })

        test("should throw error for tag containing a space", () => {
            expect(() => Tags.from(["has space"])).toThrow(/Invalid tag value/)
        })

        test("should throw error for tag containing a tab", () => {
            expect(() => Tags.from(["has\ttab"])).toThrow(/Invalid tag value/)
        })

        test("should throw error for tag containing a newline", () => {
            expect(() => Tags.from(["has\nnewline"])).toThrow(/Invalid tag value/)
        })

        test("should throw error for whitespace-only tag", () => {
            expect(() => Tags.from(["   "])).toThrow(/Invalid tag value/)
        })

        test("should throw error when any tag in array is invalid", () => {
            expect(() => Tags.from(["valid-tag", "invalid tag", "also-valid"])).toThrow(/Invalid tag value/)
        })
    })

    describe("fromObj()", () => {
        test("should create tags from a valid object", () => {
            const tags = Tags.fromObj({ courseId: "c1", studentId: "s1" })
            expect(tags.values).toEqual(["courseId=c1", "studentId=s1"])
        })

        test("should create tags from a single-entry object", () => {
            const tags = Tags.fromObj({ courseId: "c1" })
            expect(tags.values).toEqual(["courseId=c1"])
        })

        test("should throw error for empty key", () => {
            expect(() => Tags.fromObj({ "": "value" })).toThrow(/Invalid tag key/)
        })

        test("should throw error for empty value", () => {
            expect(() => Tags.fromObj({ key: "" })).toThrow(/Invalid tag value/)
        })

        test("should throw error for key containing whitespace", () => {
            expect(() => Tags.fromObj({ "course id": "c1" })).toThrow(/Invalid tag key/)
        })

        test("should throw error for value containing whitespace", () => {
            expect(() => Tags.fromObj({ courseId: "c 1" })).toThrow(/Invalid tag value/)
        })

        test("should throw error for empty object", () => {
            expect(() => Tags.fromObj({})).toThrow(/Empty object/)
        })

        test("should throw error for null object", () => {
            expect(() => Tags.fromObj(null as unknown as Record<string, string>)).toThrow(/Empty object/)
        })
    })

    describe("createEmpty()", () => {
        test("should create tags with empty values", () => {
            const tags = Tags.createEmpty()
            expect(tags.values).toEqual([])
            expect(tags.length).toBe(0)
        })
    })

    describe("length", () => {
        test("should return 0 for empty tags", () => {
            expect(Tags.createEmpty().length).toBe(0)
        })

        test("should return correct count", () => {
            expect(Tags.from(["a", "b", "c"]).length).toBe(3)
        })
    })

    describe("equals()", () => {
        test("should return true when two tags objects are identical", () => {
            const tags1 = Tags.fromObj({ courseId: "c1", studentId: "s1" })
            const tags2 = Tags.fromObj({ courseId: "c1", studentId: "s1" })
            expect(tags1.equals(tags2)).toBe(true)
        })

        test("should return true for two empty tags", () => {
            expect(Tags.createEmpty().equals(Tags.createEmpty())).toBe(true)
        })

        test("should return true for identical non-key-value tags", () => {
            const tags1 = Tags.from(["active", "priority:high"])
            const tags2 = Tags.from(["active", "priority:high"])
            expect(tags1.equals(tags2)).toBe(true)
        })

        test("should return false when tags have different lengths", () => {
            const tags1 = Tags.from(["courseId=c1"])
            const tags2 = Tags.from(["courseId=c1", "studentId=s1"])
            expect(tags1.equals(tags2)).toBe(false)
        })

        test("should return false when tags order differs", () => {
            const tags1 = Tags.from(["courseId=c1", "studentId=s1"])
            const tags2 = Tags.from(["studentId=s1", "courseId=c1"])
            expect(tags1.equals(tags2)).toBe(false)
        })

        test("should return false when tag values differ", () => {
            const tags1 = Tags.from(["courseId=c1"])
            const tags2 = Tags.from(["courseId=c2"])
            expect(tags1.equals(tags2)).toBe(false)
        })

        test("should return false comparing empty to non-empty", () => {
            expect(Tags.createEmpty().equals(Tags.from(["a"]))).toBe(false)
        })
    })
})
