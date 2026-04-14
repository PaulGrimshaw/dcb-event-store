export const ensureIsArray = <T>(input: T | T[]): T[] => (Array.isArray(input) ? input : [input])
