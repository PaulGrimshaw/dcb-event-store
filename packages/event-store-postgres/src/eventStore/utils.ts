import { Tags, DcbEvent, SequencedEvent, SequencePosition } from "@dcb-es/event-store"

export type DbWriteEvent = {
    type: string
    payload: string
    tags: string[]
}

export type DbReadEvent = {
    type: string
    payload: string
    tags: string[]
    sequence_position: string
}

export const dbEventConverter = {
    toDb: (dcbEvent: DcbEvent): DbWriteEvent => ({
        type: dcbEvent.type,
        payload: JSON.stringify({ data: dcbEvent.data, metadata: dcbEvent.metadata }),
        tags: [...dcbEvent.tags.values]
    }),
    fromDb: (dbEvent: DbReadEvent): SequencedEvent => {
        const { data, metadata } = JSON.parse(dbEvent.payload)
        return {
            position: SequencePosition.fromString(dbEvent.sequence_position),
            event: {
                type: dbEvent.type,
                data,
                metadata,
                tags: Tags.from(dbEvent.tags)
            }
        }
    }
}

export class ParamManager {
    public params: (string | string[] | number | boolean)[] = []
    add(paramValue: string | string[] | number | boolean): string {
        this.params.push(paramValue)
        return `$${this.params.length}`
    }
}
