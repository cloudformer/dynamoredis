# dynamoredis compatibility report · [tasks](TASKS.md)

Generated 2026-10-26 by `pytest tests` · dynamoredis 0.1.1 · Python 3.9.6

Each row is one feature exercised the same way on the reference and on the project; results, types and errors must match. The feature list only grows (a removed case would show in git), so the support percentage can only move up.

Whole test run: **58 passed**, 0 failed, 0 expected failures (known gaps), 0 skipped — regression suite (`tests/`) and compatibility suite (`tests/compat/`) together.

Open tasks: none — see [TASKS.md](TASKS.md).

## dynamoredis.redis vs Redis — reference: Redis 7.4.11 (Docker)

**Support 86%** (26 / 30 features) · 4 unsupported · 26 verified

✅ verified · ❌ gap — known difference, the note names the task that closes it · ⚪ quirk — documented, deliberately not aligned · ⛔ unsupported by design, refused with NotSupportedError · 🟡 fixed — xfail marker to remove · 🔴 regression · ⏭ not run

| Feature | Status | Note |
|---|---|---|
| **Strings** | | |
| APPEND / STRLEN | ✅ |  |
| INCR / DECR / INCRBY / DECRBY on missing and existing keys; counters read back as strings | ✅ |  |
| MSET / MGET with missing keys | ✅ |  |
| SET / GET / GETDEL / GETSET, missing key is None, bytes replies | ✅ |  |
| SET NX / XX / EX / PX, SETNX, SETEX | ✅ |  |
| binary-safe values: bytes that are not UTF-8 round-trip | ✅ |  |
| int and float arguments are stored as their string form; bool raises DataError | ✅ |  |
| **Keys** | | |
| DBSIZE / FLUSHDB | ✅ |  |
| DEL / EXISTS / TYPE across data types | ✅ |  |
| EXPIRE / PEXPIRE / TTL / PTTL / PERSIST; TTL of missing (-2) and persistent (-1) keys | ✅ |  |
| KEYS with glob patterns (sorted for comparison) | ✅ |  |
| RENAME, RENAME of a missing key raises | ✅ |  |
| SCAN / scan_iter with MATCH returns every key (order ignored) | ✅ |  |
| **Hashes** | | |
| HDEL / HEXISTS / HSETNX; an emptied hash disappears | ✅ |  |
| HINCRBY on missing and existing fields; on text raises | ✅ |  |
| HSET (field, mapping) / HGET / HMGET / HGETALL / HLEN / HKEYS / HVALS | ✅ |  |
| **Lists** | | |
| LPOP / RPOP with count | ✅ |  |
| LPUSH / RPUSH (multiple values) / LPOP / RPOP / LLEN; pop on empty is None | ✅ |  |
| LRANGE with negative indexes, LINDEX, LSET, LTRIM, LPUSHX on a missing key | ✅ |  |
| **Sets** | | |
| SADD / SREM / SMEMBERS / SISMEMBER / SCARD; an emptied set disappears | ✅ |  |
| **Sorted sets** | | |
| ZADD (mapping) / ZSCORE / ZCARD / ZRANGE / ZREVRANGE with scores / ZRANK | ✅ |  |
| ZADD NX / XX / CH, ZINCRBY, ZREM, ZCOUNT, ZRANGEBYSCORE with bounds and LIMIT | ✅ |  |
| **Pipeline** | | |
| pipeline(transaction=False): queued commands, shaped replies, errors inside the batch | ✅ |  |
| **Errors** | | |
| INCR on a non-integer value raises ResponseError | ✅ |  |
| WRONGTYPE: a hash command on a string key raises ResponseError | ✅ |  |
| **Unsupported** | | |
| BLPOP / BRPOP | ⛔ | needs connections held open |
| EVAL | ⛔ | server-side scripting |
| PUBLISH / SUBSCRIBE | ⛔ | needs connections held open |
| pipeline(transaction=True) = MULTI/EXEC | ⛔ | needs a session; use transaction=False |
| **General** | | |
| decode_responses=True returns str everywhere (get / hgetall / lrange / smembers / zrange) | ✅ |  |
