# Changelog

## 0.1.1 — 2026-10-26

No data is lost when two clients change the same key at once. Commands that read first now write with a
condition proving the key is still what they read; if another client changed it, the command runs again
(up to 3 retries, then `ERR concurrent modification of key '…', retried 3 times`). Each case has a
regression test that injects the other client's write between the read and the write. Server-only change:
redeploy the Lambda; the client is unchanged apart from the version.

- Expired-key cleanup deletes only while the item is still expired; a key SET again meanwhile is kept and returned.
- `GETDEL` and `GETSET` are one atomic DynamoDB operation returning the value they replaced.
- `HDEL` / `SREM` / `ZREM` delete the whole key only if no member was added meanwhile; removing what a
  concurrent command left empty drops the key instead of leaving an empty one; the reply counts exactly.
- `LPOP` / `RPOP` / `LTRIM` / `LSET` write only over the list they read (whole-content check, so a pop +
  push of the same length is detected): no pushed element is dropped, no element is popped twice, `LSET` never
  writes past the end.
- `RENAME` is one `TransactWriteItems` (write destination + delete source if unchanged): never both keys, never a stale value.
- `APPEND` and `SET … KEEPTTL` no longer drop a concurrent `APPEND` / `EXPIRE` / `PERSIST`; `APPEND` keeps the TTL, as in Redis.
- `SADD` adds every member (a concurrent `SREM` no longer makes it skip one); `HSET` / `ZADD` counts are exact;
  `HSETNX` and `ZADD NX` never overwrite a concurrent write; `ZADD XX` on a missing key no longer creates an empty one.
- `LPUSHX` / `RPUSHX` no longer recreate a key deleted meanwhile.
- No write lands in an item that expired between read and write (`INCR`, `EXPIRE`, `HSET`, pushes, …): it used
  to report success while the value stayed invisible, and `EXPIRE` could revive an expired key.
- `PERSIST` on a key deleted meanwhile returns 0 instead of a false `WRONGTYPE`.
- `DEL` is one delete per key; `FLUSHDB` keeps keys written after its scan.
- `DBSIZE` counts live keys only, like `KEYS`.
- README: expiry precision (seconds), the cost of full-table scans, the concurrency guarantee.

## 0.1.0 — 2026-10-26

First release. `from dynamoredis import redis`: the redis-py API over a Lambda Function URL, one DynamoDB
table (pay per request, TTL), ≈ $0 when idle.

- Strings (`SET` with NX/XX/EX/PX/KEEPTTL, `GET`, `GETSET`, `GETDEL`, `MGET`/`MSET`, `INCR`/`DECR`/`INCRBY`/`DECRBY`,
  `APPEND`, `STRLEN`), keys (`DEL`, `EXISTS`, `TYPE`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`, `TTL`/`PTTL`, `PERSIST`, `RENAME`,
  `KEYS`, `SCAN`, `DBSIZE`, `FLUSHDB`), hashes, lists, sets, sorted sets, `pipeline(transaction=False)`.
- Atomic where Redis is atomic: counters are DynamoDB `ADD`, pushes are `list_append`, set changes are set
  `ADD`/`DELETE`, type safety is a condition expression (`WRONGTYPE`).
- Expiry is honoured on read immediately; DynamoDB's TTL sweep deletes the item later.
- Binary-safe values; bytes replies like redis-py (`decode_responses=True` for str).
- Refused explicitly (`NotSupportedError`): pub/sub, blocking commands, Lua, `MULTI`/`EXEC`/`WATCH`, streams,
  server administration.
- Web console (RedisInsight-style browser): keys by prefix, typed values, TTL, command line, capacity units per call.
