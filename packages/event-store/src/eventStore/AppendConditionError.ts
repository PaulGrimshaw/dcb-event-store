import { AppendCondition } from "./EventStore.js"

export class AppendConditionError extends Error {
    public readonly appendCondition: AppendCondition
    public readonly commandIndex?: number

    constructor(appendCondition: AppendCondition, commandIndex?: number) {
        const indexSuffix = commandIndex !== undefined ? ` (command ${commandIndex})` : ""
        super(`Expected Version fail: New events matching appendCondition found.${indexSuffix}`)
        this.name = "AppendConditionError"
        this.appendCondition = appendCondition
        this.commandIndex = commandIndex
        Object.setPrototypeOf(this, AppendConditionError.prototype)
    }
}
