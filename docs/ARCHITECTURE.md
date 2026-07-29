# Архитектура

## Общая картина

```mermaid
flowchart TB
    writer([HTTP-клиент]):::client
    reader([WebSocket-подписчик]):::client

    subgraph APP["ПРИЛОЖЕНИЕ · N воркеров"]
        direction TB
        api["FastAPI<br/><small>роутеры, валидация</small>"]:::role
        pub["PUBLISHER<br/><small>активен один на кластер</small>"]:::accent
        tail["TAILER<br/><small>в каждом воркере</small>"]:::role
        hub[("HUB<br/><small>подписчики в памяти</small>")]:::role
        ret["RETENTION<br/><small>чистит историю</small>"]:::role
    end

    subgraph PG["POSTGRESQL"]
        direction LR
        items[("items")]:::store
        outbox[("outbox")]:::store
        lock{{"advisory lock"}}:::accent
    end

    writer ==> api
    api ==>|"одна транзакция"| items
    api ==>|"одна транзакция"| outbox

    outbox -.->|"NOTIFY outbox_new"| pub
    pub <==>|"try_advisory_xact_lock"| lock
    pub ==>|"published_at, stream_seq"| outbox

    outbox -.->|"NOTIFY outbox_published"| tail
    tail ==> hub
    hub ==> reader

    ret ==>|"DELETE старше окна"| outbox

    classDef client fill:#111827,stroke:#9ca3af,stroke-width:1.5px,color:#e5e7eb
    classDef role fill:#1f2937,stroke:#6b7280,stroke-width:1.5px,color:#e5e7eb
    classDef accent fill:#1e3a5f,stroke:#60a5fa,stroke-width:2px,color:#dbeafe
    classDef store fill:#111827,stroke:#60a5fa,stroke-width:1.5px,color:#dbeafe

    style APP fill:#0f1520,stroke:#374151,stroke-width:1px,color:#9ca3af
    style PG fill:#0f1520,stroke:#374151,stroke-width:1px,color:#9ca3af
```

Publisher решает порядок и работает в одном экземпляре на кластер. Tailer доставляет и работает
в каждом воркере. Без такого разделения события, разобранные одним процессом,
попадали бы только в его собственный хаб.

## Путь записи

```mermaid
sequenceDiagram
    autonumber
    participant C as Клиент
    participant A as FastAPI
    participant DB as PostgreSQL
    participant P as Publisher
    participant T as Tailer
    participant S as Подписчик

    C->>A: POST /api/v1/items
    A->>DB: BEGIN
    A->>DB: INSERT INTO items
    A->>DB: INSERT INTO outbox
    A->>DB: COMMIT
    Note over DB: NOTIFY доставляется<br/>только теперь.<br/>Откат не шлёт ничего
    A-->>C: 201 { item, event_id }

    DB-->>P: NOTIFY outbox_new
    Note over P: пауза 5 мс,<br/>чтобы схлопнуть пачку
    P->>DB: try_advisory_xact_lock
    P->>DB: SELECT … WHERE published_at IS NULL<br/>FOR UPDATE SKIP LOCKED
    P->>DB: UPDATE published_at, stream_seq
    Note over P: COMMIT снимает лок

    DB-->>T: NOTIFY outbox_published
    T->>DB: SELECT … WHERE stream_seq > cursor
    T->>S: событие в WebSocket
```

`NOTIFY` транзакционен. Postgres кладёт уведомление в очередь и
доставляет слушателям, только если транзакция закоммитилась. Откатившаяся не
шлёт ничего.

Publisher читает в свежем снапшоте. Незакоммиченные строки ему
попросту не видны, поэтому опубликовать данные раньше их коммита физически
невозможно.

Лок держится до `COMMIT`, поэтому транзакции publisher'ов не
пересекаются, и выданные ими блоки `stream_seq` коммитятся в том же порядке, в
каком были зарезервированы. На это опирается курсор tailer'а.

## Реконнект клиента

```mermaid
sequenceDiagram
    autonumber
    participant S as Подписчик
    participant W as WebSocket-эндпоинт
    participant DB as PostgreSQL
    participant H as Hub

    Note over S: соединение оборвалось<br/>на stream_seq = 41

    S->>W: connect ?last_event_id=41
    W->>DB: min(stream_seq) - жив ли курсор?

    alt курсор выпал за окно retention
        W-->>S: cursor_too_old + close 4003
        Note over S: полный resync<br/>через GET /items
    else курсор в окне
        W->>H: subscribe (ДО реплея)
        Note over W,H: подписка раньше догона:<br/>события, пришедшие во время<br/>реплея, буферизуются,<br/>а не теряются
        W-->>S: ready
        W->>DB: SELECT … WHERE stream_seq > 41
        W-->>S: пропущенные события
        W-->>S: replay_complete
        W-->>S: живые события,<br/>всё ≤ 41 отбрасывается
    end
```

Порядок «сначала подписка, потом реплей» неслучаен. Наоборот было бы окно, в
котором событие уже не попало в реплей и ещё не попало в подписку. Пересечение
двух путей даёт дубликаты, которые клиент отбрасывает по курсору; разрыв дал бы
потерю, которую он не заметил бы.

## Что произойдёт при сбое

```mermaid
flowchart LR
    A["Транзакция упала<br/>до COMMIT"]:::case --> A1["Нет ни строки,<br/>ни события"]:::ok
    B["Процесс умер<br/>до публикации"]:::case --> B1["Строки остались<br/>published_at IS NULL,<br/>следующий запуск разберёт"]:::ok
    C["Процесс умер после<br/>публикации, до рассылки"]:::case --> C1["События durable<br/>с курсором, клиент<br/>доберёт реплеем"]:::ok
    D["NOTIFY потерян"]:::case --> D1["Страховочный<br/>поллинг 200 мс"]:::ok
    E["Клиент не успевает"]:::case --> E1["Очередь переполнилась,<br/>close 4001,<br/>реконнект + реплей"]:::ok
    F["Publisher откатился<br/>после резерва блока"]:::case --> F1["Дыра в stream_seq<br/>навсегда. Tailer не ждёт<br/>непрерывности"]:::note

    classDef case fill:#1f2937,stroke:#6b7280,stroke-width:1.5px,color:#e5e7eb
    classDef ok fill:#111827,stroke:#34d399,stroke-width:1.5px,color:#d1fae5
    classDef note fill:#111827,stroke:#60a5fa,stroke-width:1.5px,color:#dbeafe
```

Зелёная рамка - потерь нет. Синяя - единственный случай с последствием, и оно
безвредно: дыра в последовательности не мешает, потому что курсор клиента и
курсор tailer'а сравнивают «больше чем», а не «следующий за».

## Схема данных

```mermaid
erDiagram
    items {
        uuid id PK
        text name
        integer value
        integer version "оптимистичная блокировка"
        timestamptz created_at
        timestamptz updated_at
    }
    outbox {
        bigserial id PK "порядок вставки"
        text aggregate_type
        uuid aggregate_id "items.id, БЕЗ FK"
        text event_type
        jsonb payload "полный снимок"
        timestamptz created_at
        timestamptz published_at "NULL = в очереди"
        bigint stream_seq "курсор клиента, порядок коммитов"
    }
    items ||..o{ outbox : "по значению"
```

Связь пунктиром: внешнего ключа нет намеренно. Событие `item.deleted` обязано
пережить строку, о которой рассказывает, - иначе подписчик не узнает об
удалении.

## Почему два разных идентификатора

```mermaid
sequenceDiagram
    participant T1 as Транзакция A
    participant T2 as Транзакция B
    participant P as Publisher

    T1->>T1: INSERT outbox → id = 7
    T2->>T2: INSERT outbox → id = 10
    T2->>T2: COMMIT
    Note over T2: id = 10 виден
    T1->>T1: COMMIT
    Note over T1: id = 7 виден ПОЗЖЕ

    P->>P: stream_seq: 10 → 1
    P->>P: stream_seq: 7 → 2
    Note over P: порядок доставки<br/>монотонен
```

`outbox.id` выдаётся при `INSERT`, до коммита. Клиент, дошедший до id = 10, всё
ещё должен получить id = 7 - а курсор по `id` при реконнекте молча бы его
пропустил. Это не теоретическая тонкость, а прямое нарушение требования про
reconnection и deduplication.

`stream_seq` присваивает publisher после коммита, поэтому он монотонен в
порядке доставки - единственном порядке, с которого клиент может продолжить.

`event_id` (он же `outbox.id`) остаётся в API как ручка корреляции: `POST`
возвращает его, и то же значение приходит в событии, так что клиент может
связать свою запись с её событием. Курсором он не является.

## Роли и их ресурсы

| Роль | Экземпляров | Соединение | Что делает |
| --- | --- | --- | --- |
| FastAPI | по числу воркеров | пул (40) | HTTP и WebSocket |
| Publisher | **1 активный** на кластер | своё | Назначает `stream_seq` |
| Tailer | по числу воркеров | своё | Читает и раздаёт в хаб |
| Retention | по числу воркеров | своё | Чистит историю |

Фоновые роли работают на собственных соединениях, а не берут их из пула
запросов. Под всплеском записей пул занят самими писателями, и цикл в очереди
за ними добавляет ровно ту задержку, ради устранения которой существует. До
разделения замеры показывали p95 около 700 мс, после - десятки миллисекунд.

При `APP_WORKERS=4` и `DB_POOL_MAX_SIZE=40` это 4 × (40 + 3) = 172 соединения;
в compose у Postgres выставлено `max_connections=300`.
