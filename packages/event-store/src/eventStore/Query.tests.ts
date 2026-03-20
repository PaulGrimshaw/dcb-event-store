import { Query, QueryItem } from "./Query"
import { Tags } from "./Tags"

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
})
