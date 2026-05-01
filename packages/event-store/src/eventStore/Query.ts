import { Tags } from "./Tags.js"

export interface QueryItem {
    tags?: Tags
    types: string[]
}

export class Query {
    #items: QueryItem[]
    #isAll: boolean

    private constructor(queryItems: QueryItem[] | "All") {
        this.#isAll = queryItems === "All"
        this.#items = queryItems === "All" ? [] : queryItems
    }

    static all() {
        return new Query("All")
    }

    static fromItems(queryItems: QueryItem[]) {
        if (!Array.isArray(queryItems) || queryItems.length === 0) {
            throw new Error("Query must be 'All' or a non-empty array of QueryItems")
        }
        for (let i = 0; i < queryItems.length; i++) {
            const item = queryItems[i]
            if (!Array.isArray(item.types) || item.types.length === 0) {
                throw new Error(
                    `QueryItem at index ${i} must have a non-empty types array; tag-only filters are not supported. Use Query.all() to match every event.`
                )
            }
        }
        return new Query(queryItems)
    }

    get items() {
        if (this.#isAll) throw new Error("Cannot access items on 'All' query")
        return this.#items
    }

    get isAll() {
        return this.#isAll
    }
}
