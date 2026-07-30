# Architecture

## Overview

```mermaid
flowchart TB
    writer([HTTP client]):::client
    reader([WebSocket subscriber]):::client

    subgraph APP["APPLICATION · N workers"]
        direction TB
        api["FastAPI<br/><small>routers, validation</small>"]:::role
        pub["PUBLISHER<br/><small>one active per cluster</small>"]:::accent
        tail["TAILER<br/><small>in every worker</small>"]:::role
        hub[("HUB<br/><small>subscribers in memory</small>")]:::role
        ret["RETENTION<br/><small>trims history</small>"]:::role
    end

    subgraph PG["POSTGRESQL"]
        direction LR
        items[("items")]:::store
        outbox[("outbox")]:::store
        lock{{"advisory lock"}}:::accent
    end

    writer ==> api
    api ==>|"one transaction"| items
    api ==>|"one transaction"| outbox

    outbox -.->|"NOTIFY outbox_new"| pub
    pub <==>|"try_advisory_xact_lock"| lock
    pub ==>|"published_at, stream_seq"| outbox

    outbox -.->|"NOTIFY outbox_published"| tail
    tail ==> hub
    hub ==> reader

    ret ==>|"DELETE older than window"| outbox

    classDef client fill:#111827,stroke:#9ca3af,stroke-width:1.5px,color:#e5e7eb
    classDef role fill:#1f2937,stroke:#6b7280,stroke-width:1.5px,color:#e5e7eb
    classDef accent fill:#1e3a5f,stroke:#60a5fa,stroke-width:2px,color:#dbeafe
    classDef store fill:#111827,stroke:#60a5fa,stroke-width:1.5px,color:#dbeafe

    style APP fill:#0f1520,stroke:#374151,stroke-width:1px,color:#9ca3af
    style PG fill:#0f1520,stroke:#374151,stroke-width:1px,color:#9ca3af
```

The publisher decides the order and runs as a single instance per cluster. The
tailer delivers and runs in every worker. Without that split, events consumed by
one process would only ever reach its own hub.

## Write path

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as FastAPI
    participant DB as PostgreSQL
    participant P as Publisher
    participant T as Tailer
    participant S as Subscriber

    C->>A: POST /api/v1/items
    A->>DB: BEGIN
    A->>DB: INSERT INTO items
    A->>DB: INSERT INTO outbox
    A->>DB: COMMIT
    Note over DB: NOTIFY is delivered<br/>only now.<br/>A rollback sends nothing
    A-->>C: 201 { item, event_id }

    DB-->>P: NOTIFY outbox_new
    Note over P: 5 ms pause<br/>to collapse the burst
    P->>DB: try_advisory_xact_lock
    P->>DB: SELECT … WHERE published_at IS NULL<br/>FOR UPDATE SKIP LOCKED
    P->>DB: UPDATE published_at, stream_seq
    Note over P: COMMIT releases the lock

    DB-->>T: NOTIFY outbox_published
    T->>DB: SELECT … WHERE stream_seq > cursor
    T->>S: event over WebSocket
```

`NOTIFY` is transactional. Postgres queues the notification and delivers it to
listeners only if the transaction committed. A rolled-back one sends nothing.

The publisher reads in a fresh snapshot. Uncommitted rows are simply invisible
to it, so publishing data before its commit is physically impossible.

The lock is held until `COMMIT`, so publisher transactions never overlap, and
the `stream_seq` blocks they hand out commit in the same order they were
reserved. The tailer's cursor relies on that.

## Client reconnect

```mermaid
sequenceDiagram
    autonumber
    participant S as Subscriber
    participant W as WebSocket endpoint
    participant DB as PostgreSQL
    participant H as Hub

    Note over S: connection dropped<br/>at stream_seq = 41

    S->>W: connect ?last_event_id=41
    W->>DB: min(stream_seq) - is the cursor alive?

    alt cursor fell outside the retention window
        W-->>S: cursor_too_old + close 4003
        Note over S: full resync<br/>via GET /items
    else cursor inside the window
        W->>H: subscribe (BEFORE the replay)
        Note over W,H: subscribe before catching up:<br/>events landing during<br/>the replay are buffered,<br/>not lost
        W-->>S: ready
        W->>DB: SELECT … WHERE stream_seq > 41
        W-->>S: missed events
        W-->>S: replay_complete
        W-->>S: live events,<br/>anything ≤ 41 is discarded
    end
```

The order "subscribe first, replay second" is not accidental. The other way
round leaves a window in which an event is already past the replay and not yet
in the subscription. The overlap between the two paths produces duplicates,
which the client discards by cursor; a gap would produce a loss it would not
notice.

## What happens on failure

```mermaid
flowchart LR
    A["Transaction failed<br/>before COMMIT"]:::case --> A1["Neither the row<br/>nor the event exists"]:::ok
    B["Process died<br/>before publishing"]:::case --> B1["Rows stayed<br/>published_at IS NULL,<br/>the next run picks them up"]:::ok
    C["Process died after publishing,<br/>before fan-out"]:::case --> C1["Events are durable<br/>with a cursor, the client<br/>catches up by replay"]:::ok
    D["NOTIFY lost"]:::case --> D1["Safety-net<br/>poll every 200 ms"]:::ok
    E["Client cannot keep up"]:::case --> E1["Queue overflowed,<br/>close 4001,<br/>reconnect + replay"]:::ok
    F["Publisher rolled back<br/>after reserving a block"]:::case --> F1["Permanent hole in<br/>stream_seq. The tailer does<br/>not require contiguity"]:::note

    classDef case fill:#1f2937,stroke:#6b7280,stroke-width:1.5px,color:#e5e7eb
    classDef ok fill:#111827,stroke:#34d399,stroke-width:1.5px,color:#d1fae5
    classDef note fill:#111827,stroke:#60a5fa,stroke-width:1.5px,color:#dbeafe
```

A green border means nothing is lost. Blue is the one case with a consequence,
and it is harmless: a hole in the sequence does not matter, because the client's
cursor and the tailer's cursor compare "greater than", not "next after".

## Data model

```mermaid
erDiagram
    items {
        uuid id PK
        text name
        integer value
        integer version "optimistic locking"
        timestamptz created_at
        timestamptz updated_at
    }
    outbox {
        bigserial id PK "insert order"
        text aggregate_type
        uuid aggregate_id "items.id, NO FK"
        text event_type
        jsonb payload "full snapshot"
        timestamptz created_at
        timestamptz published_at "NULL = queued"
        bigint stream_seq "client cursor, commit order"
    }
    items ||..o{ outbox : "by value"
```

The dashed relation: there is deliberately no foreign key. An `item.deleted`
event has to outlive the row it describes, otherwise a subscriber never learns
about the deletion.

## Why two different identifiers

```mermaid
sequenceDiagram
    participant T1 as Transaction A
    participant T2 as Transaction B
    participant P as Publisher

    T1->>T1: INSERT outbox → id = 7
    T2->>T2: INSERT outbox → id = 10
    T2->>T2: COMMIT
    Note over T2: id = 10 is visible
    T1->>T1: COMMIT
    Note over T1: id = 7 becomes visible LATER

    P->>P: stream_seq: 10 → 1
    P->>P: stream_seq: 7 → 2
    Note over P: delivery order<br/>is monotonic
```

`outbox.id` is assigned at `INSERT`, before the commit. A client that reached
id = 10 is still owed id = 7 - and a cursor over `id` would silently skip it on
reconnect. This is not a theoretical subtlety but a direct violation of the
reconnection and deduplication requirement.

`stream_seq` is assigned by the publisher after the commit, so it is monotonic
in delivery order - the only order a client can resume from.

`event_id` (which is `outbox.id`) stays in the API as a correlation handle:
`POST` returns it, and the same value arrives in the event, so a client can tie
its own write to the event it produced. It is not a cursor.

## Roles and their resources

| Role | Instances | Connection | What it does |
| --- | --- | --- | --- |
| FastAPI | one per worker | pool (40) | HTTP and WebSocket |
| Publisher | **1 active** per cluster | its own | Assigns `stream_seq` |
| Tailer | one per worker | its own | Reads and fans out to the hub |
| Retention | one per worker | its own | Trims history |

The background roles run on their own connections rather than borrowing from the
request pool. Under a write burst the pool is saturated by the writers
themselves, and a loop queued behind them adds exactly the latency it exists to
remove. Before the split, measurements showed p95 around 700 ms; after it, tens
of milliseconds.

With `APP_WORKERS=4` and `DB_POOL_MAX_SIZE=40` that is 4 × (40 + 3) = 172
connections; compose sets `max_connections=300` on Postgres.
