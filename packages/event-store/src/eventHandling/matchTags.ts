import { Tags } from "../eventStore/Tags.js"

export const ensureIsArray = (stringOrStringArray: string | string[]) =>
    Array.isArray(stringOrStringArray) ? stringOrStringArray : [stringOrStringArray]

export const matchTags = ({ tags, tagFilter }: { tags: Tags; tagFilter: Tags | undefined }): boolean => {
    if (!tagFilter) return true

    //query item has tags that the event does not
    if (tagFilter.values.length > 0 && tagFilter.values.some(t => !tags.values.includes(t))) return false

    //otherwise query has tags that match event
    return true
}
