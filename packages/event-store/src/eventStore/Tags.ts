const validateTagValues = (values: string[]): void => {
    const regex = /^\S+$/
    values.forEach(val => {
        if (!regex.test(val)) throw new Error(`Invalid tag value: ${val}`)
    })
}

export class Tags {
    #value: string[]

    private constructor(value: string[]) {
        this.#value = value
    }

    public get values(): string[] {
        return this.#value
    }

    public get length(): number {
        return this.#value.length
    }

    public equals = (other: Tags): boolean =>
        this.#value.length === other.#value.length && this.#value.every((val, idx) => val === other.#value[idx])

    public static from(values: string[]): Tags {
        validateTagValues(values)
        return new Tags(values)
    }

    public static fromObj(obj: Record<string, string>): Tags {
        if (!obj || Object.keys(obj).length === 0)
            throw new Error("Empty object is not valid for fromObj factory method")

        const regex = /^\S+$/
        Object.entries(obj).forEach(([key, value]) => {
            if (!key || !regex.test(key)) throw new Error(`Invalid tag key: "${key}"`)
            if (!value || !regex.test(value)) throw new Error(`Invalid tag value: "${value}" for key "${key}"`)
        })

        const values = Object.keys(obj).map(key => `${key}=${obj[key]}`)
        return new Tags(values)
    }

    public static createEmpty(): Tags {
        return new Tags([])
    }
}
