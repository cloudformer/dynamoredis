# Changelog

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
