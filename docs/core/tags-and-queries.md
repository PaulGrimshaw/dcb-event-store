# Tags, Queries, and SequencePosition

API reference for the filtering and ordering primitives in `@dcb-es/event-store`. These types are used to scope reads, define consistency boundaries, and track position in the event stream.

---

## Tags

```ts
class Tags {
  get values(): string[]
  get length(): number
  equals(other: Tags): boolean

  static from(values: string[]): Tags
  static fromObj(obj: Record<string, string>): Tags
  static createEmpty(): Tags
}
```

An immutable, ordered collection of tag values. Tags are the primary mechanism for scoping events in the DCB pattern -- they determine which events an [AppendCondition](event-store-interface.md#appendcondition) or [Query](#query) matches.

### Factory methods

#### `Tags.from(values: string[])`

Creates tags from an array of raw string values. Each value must be non-empty and contain no whitespace (`/^\S+$/`).

```ts
const tags = Tags.from(["courseId=cs101", "studentId=42"])
```

#### `Tags.fromObj(obj: Record<string, string>)`

Creates tags from a key-value object. Keys and values must be non-empty and contain no whitespace. Each entry becomes a `key=value` string.

```ts
const tags = Tags.fromObj({ courseId: "cs101", studentId: "42" })
// tags.values => ["courseId=cs101", "studentId=42"]
```

Throws if the object is empty or if any key or value is empty or contains whitespace.

#### `Tags.createEmpty()`

Creates a `Tags` instance with no values. Useful for events that need no tag-based filtering.

```ts
const empty = Tags.createEmpty()
// empty.length => 0
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `values` | `string[]` | The raw tag strings. |
| `length` | `number` | Number of tags. |

### Methods

#### `equals(other: Tags): boolean`

Strict equality -- same length and same values in the same order.

### Validation rules

- Tag values (whether created via `from` or `fromObj`) must match `/^\S+$/` -- non-empty, no whitespace.
- `fromObj` additionally validates that keys are non-empty and contain no whitespace.
- `fromObj` throws on an empty object.

---

## Query

```ts
class Query {
  get isAll(): boolean
  get items(): QueryItem[]

  static all(): Query
  static fromItems(queryItems: QueryItem[]): Query
}
```

Defines which events to read from the store. A `Query` is either "all events" or a non-empty array of `QueryItem`.

### Factory methods

#### `Query.all()`

Matches every event in the store. Cannot be used in an [AppendCondition](event-store-interface.md#appendcondition) -- only for reads and subscriptions.

```ts
const allEvents = eventStore.read(Query.all())
```

#### `Query.fromItems(queryItems: QueryItem[])`

Creates a query from one or more `QueryItem`. Throws if the array is empty.

```ts
const query = Query.fromItems([
  { types: ["courseWasRegistered", "courseCapacityWasChanged"], tags: Tags.fromObj({ courseId: "cs101" }) },
  { types: ["studentWasSubscribed"], tags: Tags.fromObj({ courseId: "cs101" }) }
])
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `isAll` | `boolean` | `true` if this is a `Query.all()` query. |
| `items` | `QueryItem[]` | The query items. Throws if accessed on a `Query.all()`. |

---

## QueryItem

```ts
interface QueryItem {
  tags?: Tags
  types?: string[]
}
```

A single filter criterion within a [Query](#query).

| Field | Description |
|-------|-------------|
| `types` | Event type names to match. An event matches if its `type` is in this array. |
| `tags` | Tag filter. An event matches if its tags are a **superset** of the filter tags (i.e. the event has at least all the tags specified in the filter). |

### Matching semantics

**Within a QueryItem** -- both `types` and `tags` must match (AND):

- The event's `type` must be one of the listed `types`.
- The event's tags must include all of the QueryItem's tags (subset matching). The event may have additional tags.

**Across QueryItems** -- items are combined with OR. An event matches the Query if it matches **any** QueryItem.

**Tag subset matching** -- a QueryItem with `tags: Tags.fromObj({ courseId: "cs101" })` matches events tagged with `{ courseId: "cs101" }` and also events tagged with `{ courseId: "cs101", studentId: "42" }`, because the event's tags are a superset of the filter tags.

### Example

```ts
// Matches:
//   - courseWasRegistered events with courseId=cs101
//   - OR studentWasSubscribed events with studentId=42
const query = Query.fromItems([
  { types: ["courseWasRegistered"], tags: Tags.fromObj({ courseId: "cs101" }) },
  { types: ["studentWasSubscribed"], tags: Tags.fromObj({ studentId: "42" }) }
])
```

### Append condition constraints

When used inside an [AppendCondition](event-store-interface.md#appendcondition), every `QueryItem` must have at least one type **and** at least one tag. This ensures the consistency boundary is scoped -- see [validateAppendCondition](event-store-interface.md#validateappendcondition).

---

## SequencePosition

```ts
class SequencePosition {
  isAfter(other: SequencePosition): boolean
  isBefore(other: SequencePosition): boolean
  equals(other: SequencePosition): boolean
  toString(): string

  static initial(): SequencePosition
  static fromString(s: string): SequencePosition
  static compare(a: SequencePosition, b: SequencePosition): number
}
```

An opaque, comparable position in the global event stream. Every event in the store has a unique `SequencePosition` that defines its total order relative to all other events.

### Factory methods

#### `SequencePosition.initial()`

Returns the position representing "before any events". Use this as the starting point for reads and subscriptions.

```ts
const start = SequencePosition.initial()
```

#### `SequencePosition.fromString(s: string)`

Parses a position from its string representation. Throws if the string is not a valid non-negative integer.

```ts
const pos = SequencePosition.fromString("42")
```

### Instance methods

| Method | Returns | Description |
|--------|---------|-------------|
| `isAfter(other)` | `boolean` | `true` if this position is strictly after `other`. |
| `isBefore(other)` | `boolean` | `true` if this position is strictly before `other`. |
| `equals(other)` | `boolean` | `true` if both positions are the same. |
| `toString()` | `string` | String representation. Round-trips with `fromString`. |

### Static methods

#### `SequencePosition.compare(a, b)`

Comparator function suitable for sorting. Returns `-1`, `0`, or `1`.

```ts
const sorted = positions.sort(SequencePosition.compare)
```

### Design note

`SequencePosition` is intentionally opaque -- consumers should not assume anything about the internal representation. Use the comparison methods rather than parsing the string value. Different store implementations may use different position schemes (integers, timestamps, composite keys).
