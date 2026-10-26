# Tasks

The numbers [REPORT.md](REPORT.md) refers to in its gap notes. Open first; done ones stay below so old
notes keep resolving. Numbers are never reused.

## Open

| task | what |
|---|---|
| 1 | `pipeline(transaction=True)` as one DynamoDB `TransactWriteItems` (≤ 100 write commands, no reads inside) |
| 2 | more commands: `SETRANGE/GETRANGE`, `HSTRLEN`, `LREM/LINSERT/LPOS`, `SMOVE/SINTER/SUNION/SDIFF`, `ZPOPMIN/ZPOPMAX/ZREMRANGEBYSCORE/ZUNIONSTORE`, `EXPIRETIME`, `COPY`, `RANDOMKEY` |
| 3 | `INCRBYFLOAT` (floats are stored as text today) |
| 4 | `KEYS` / `DBSIZE` / `FLUSHDB` scan the whole table: document the cost, add a GSI by key prefix if it matters |

Permanent, not tasks: ⚪ quirks and ⛔ unsupported — pub/sub, blocking commands (`BLPOP` …), Lua / functions,
`WATCH` / `MULTI` / `EXEC`, streams, `CONFIG` / `CLIENT` / replication / cluster commands.

## Done

| task | what | release |
|---|---|---|
