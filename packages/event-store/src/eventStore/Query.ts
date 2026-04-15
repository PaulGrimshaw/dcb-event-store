import { Tags } from "./Tags.js"

export interface QueryItem {
    tags?: Tags
    types?: string[]
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
