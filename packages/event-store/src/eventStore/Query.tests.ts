import { Query, QueryItem } from "./Query.js"
import { Tags } from "./Tags.js"

describe("Query", () => {
    test("should create query with 'All' type using Query.all", () => {
        const query = Query.all()
        expect(query.isAll).toBe(true)
    })

    test("should create query from valid query items using Query.fromItems", () => {
        const items: QueryItem[] = [
            { types: ["click"] },
            {
                tags: Tags.fromObj({ courseId: "c1" }),
                types: ["hover"]
            }
        ]
        const query = Query.fromItems(items)
        expect(query.isAll).toBe(false)
        expect(query.items).toEqual(items)
    })

    test("should throw an error when undefined or null is passed to Query.fromItems", () => {
        expect(() => Query.fromItems(undefined as unknown as QueryItem[])).toThrow(
            "Query must be 'All' or a non-empty array of QueryItems"
        )
    })

    test("should throw error when empty array is passed to Query.fromItems", () => {
        expect(() => Query.fromItems([])).toThrow("Query must be 'All' or a non-empty array of QueryItems")
    })

    test("should throw error when non-array is passed to Query.fromItems", () => {
        expect(() => Query.fromItems("hello" as unknown as QueryItem[])).toThrow(
            "Query must be 'All' or a non-empty array of QueryItems"
        )
    })

    test("should throw error when items is accessed in 'all' mode", () => {
        const query = Query.all()
        expect(() => query.items).toThrow("Cannot access items on 'All' query")
    })

    test("should throw error when an item omits types (tag-only filter)", () => {
        expect(() => Query.fromItems([{ tags: Tags.fromObj({ e: "1" }) } as unknown as QueryItem])).toThrow(
            "non-empty types array"
        )
    })

    test("should throw error when an item's types is an empty array", () => {
        expect(() => Query.fromItems([{ types: [] }])).toThrow("non-empty types array")
    })
})
