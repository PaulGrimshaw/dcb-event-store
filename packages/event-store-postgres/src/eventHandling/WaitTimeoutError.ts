export class WaitTimeoutError extends Error {
    constructor(handlerName: string, position: string, timeoutMs: number) {
        super(`Timeout: handler "${handlerName}" did not reach position ${position} within ${timeoutMs}ms`)
        this.name = "WaitTimeoutError"
        Object.setPrototypeOf(this, WaitTimeoutError.prototype)
    }
}
