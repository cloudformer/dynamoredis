"""dynamoredis.redis vs a real Redis 7 (Docker): the compatibility matrix.

One test = one feature; docstring "Category | Feature | note". The list only grows. Run with:

    docker compose -f tests/compat/docker-compose.yml up -d redis
    REDIS_URL=redis://127.0.0.1:6379/0 python3 -m pytest tests -q
    COMPAT_DRY=1 python3 -m pytest tests/compat -q -s      # dynamoredis side only, no Docker
"""
import pytest

# ---- strings ----

def test_set_get(pair):
    """Strings | SET / GET / GETDEL / GETSET, missing key is None, bytes replies"""
    pair([("set", "k", "v"), ("get", "k"), ("get", "nope"), ("getset", "k", "w"), ("getdel", "k"), ("get", "k")])


def test_set_options(pair):
    """Strings | SET NX / XX / EX / PX, SETNX, SETEX"""
    pair([("set", "k", "1", {"nx": True}), ("set", "k", "2", {"nx": True}), ("get", "k"),
          ("set", "new", "x", {"xx": True}), ("set", "k", "3", {"xx": True}), ("get", "k"),
          ("set", "t", "v", {"ex": 100}), ("setex", "t2", 100, "v"), ("set", "t3", "v", {"px": 100000}),
          ("setnx", "k", "9"), ("setnx", "k4", "9"), ("get", "k4")])


def test_counters(pair):
    """Strings | INCR / DECR / INCRBY / DECRBY on missing and existing keys; counters read back as strings"""
    pair([("incr", "n"), ("incr", "n"), ("incrby", "n", 10), ("decr", "n"), ("decrby", "n", 3), ("get", "n"),
          ("set", "m", "7"), ("incr", "m"), ("set", "neg", "-5"), ("incr", "neg")])


def test_incr_on_text_is_an_error(pair):
    """Errors | INCR on a non-integer value raises ResponseError"""
    pair([("set", "k", "abc"), ("incr", "k")])


def test_mget_mset(pair):
    """Strings | MSET / MGET with missing keys"""
    pair([("mset", {"a": "1", "b": "2"}), ("mget", ["a", "b", "c"]), ("mget", "a")])


def test_append_strlen(pair):
    """Strings | APPEND / STRLEN"""
    pair([("append", "k", "ab"), ("append", "k", "cd"), ("get", "k"), ("strlen", "k"), ("strlen", "nope")])


def test_binary_values(pair):
    """Strings | binary-safe values: bytes that are not UTF-8 round-trip"""
    pair([("set", "b", b"\x00\x01\xff"), ("get", "b"), ("set", "u", "héllo"), ("get", "u")])


def test_numbers_as_values(pair):
    """Strings | int and float arguments are stored as their string form; bool raises DataError"""
    pair([("set", "i", 42), ("get", "i"), ("set", "f", 1.5), ("get", "f"), ("set", "t", True), ("get", "t")])


# ---- keys ----

def test_del_exists_type(pair):
    """Keys | DEL / EXISTS / TYPE across data types"""
    pair([("set", "s", "v"), ("hset", "h", "f", "v"), ("rpush", "l", "v"), ("sadd", "z", "v"), ("zadd", "zs", {"m": 1}),
          ("exists", "s", "h", "nope"), ("type", "s"), ("type", "h"), ("type", "l"), ("type", "z"), ("type", "zs"),
          ("type", "nope"), ("delete", "s", "h", "nope"), ("exists", "s")])


def test_expire_ttl_persist(pair):
    """Keys | EXPIRE / PEXPIRE / TTL / PTTL / PERSIST; TTL of missing (-2) and persistent (-1) keys"""
    pair([("set", "k", "v"), ("ttl", "k"), ("ttl", "nope"), ("expire", "k", 1000), ("persist", "k"), ("ttl", "k"),
          ("expire", "nope", 10), ("persist", "k"), ("pexpire", "k", 100000), ("pttl", "nope")])
    real, ours = pair.both(("ttl", "k"))
    assert ours.value in (99, 100), ours


def test_rename(pair):
    """Keys | RENAME, RENAME of a missing key raises"""
    pair([("set", "a", "v"), ("rename", "a", "b"), ("get", "a"), ("get", "b"), ("rename", "nope", "x")])


def test_keys_pattern(pair):
    """Keys | KEYS with glob patterns (sorted for comparison)"""
    pair([("set", "user:1", "a"), ("set", "user:2", "b"), ("set", "order:1", "c")])
    for pattern in ("user:*", "*", "user:?", "order*"):
        real, ours = pair.both(("keys", pattern))
        assert sorted(real.value) == sorted(ours.value) if real is not ours else True, pattern


def test_scan_iter(pair):
    """Keys | SCAN / scan_iter with MATCH returns every key (order ignored)"""
    pair([("set", "a:1", "x"), ("set", "a:2", "x"), ("set", "b:1", "x")])
    real, ours = pair.both(("scan_iter", {"match": "a:*"}))
    assert sorted(real.value) == sorted(ours.value) if real is not ours else len(ours.value) == 2


def test_dbsize_flushdb(pair):
    """Keys | DBSIZE / FLUSHDB"""
    pair([("set", "a", "1"), ("set", "b", "2"), ("dbsize",), ("flushdb",), ("dbsize",), ("get", "a")])


# ---- hashes ----

def test_hash_basics(pair):
    """Hashes | HSET (field, mapping) / HGET / HMGET / HGETALL / HLEN / HKEYS / HVALS"""
    pair([("hset", "h", "a", "1"), ("hset", "h", {"mapping": {"b": "2", "c": "3"}}), ("hset", "h", "a", "9"),
          ("hget", "h", "a"), ("hget", "h", "nope"), ("hmget", "h", ["a", "nope", "c"]), ("hgetall", "h"),
          ("hgetall", "nope"), ("hlen", "h")])
    for m in ("hkeys", "hvals"):
        real, ours = pair.both((m, "h"))
        assert sorted(real.value) == sorted(ours.value) if real is not ours else True, m


def test_hash_delete_exists_setnx(pair):
    """Hashes | HDEL / HEXISTS / HSETNX; an emptied hash disappears"""
    pair([("hset", "h", "a", "1"), ("hset", "h", "b", "2"), ("hexists", "h", "a"), ("hexists", "h", "z"),
          ("hsetnx", "h", "a", "x"), ("hsetnx", "h", "c", "x"), ("hdel", "h", "a", "z"), ("hdel", "h", "b", "c"),
          ("exists", "h"), ("hdel", "nope", "a")])


def test_hincrby(pair):
    """Hashes | HINCRBY on missing and existing fields; on text raises"""
    pair([("hincrby", "h", "n", 5), ("hincrby", "h", "n", -2), ("hget", "h", "n"), ("hset", "h", "t", "x"), ("hincrby", "h", "t", 1)])


# ---- lists ----

def test_list_push_pop(pair):
    """Lists | LPUSH / RPUSH (multiple values) / LPOP / RPOP / LLEN; pop on empty is None"""
    pair([("lpush", "l", "a", "b"), ("rpush", "l", "c", "d"), ("lrange", "l", 0, -1), ("llen", "l"),
          ("lpop", "l"), ("rpop", "l"), ("lrange", "l", 0, -1), ("lpop", "l"), ("lpop", "l"), ("lpop", "l"), ("exists", "l")])


def test_list_pop_count(pair):
    """Lists | LPOP / RPOP with count"""
    pair([("rpush", "l", "a", "b", "c", "d"), ("lpop", "l", 2), ("rpop", "l", 5), ("lpop", "nope", 2)])


def test_list_range_index_set_trim(pair):
    """Lists | LRANGE with negative indexes, LINDEX, LSET, LTRIM, LPUSHX on a missing key"""
    pair([("rpush", "l", "a", "b", "c", "d", "e"), ("lrange", "l", 1, 3), ("lrange", "l", -2, -1), ("lrange", "l", 3, 1),
          ("lindex", "l", 0), ("lindex", "l", -1), ("lindex", "l", 99), ("lset", "l", 1, "B"), ("lrange", "l", 0, -1),
          ("ltrim", "l", 1, 2), ("lrange", "l", 0, -1), ("lpushx", "nope", "x"), ("exists", "nope"), ("lset", "nope", 0, "x")])


# ---- sets ----

def test_set_ops(pair):
    """Sets | SADD / SREM / SMEMBERS / SISMEMBER / SCARD; an emptied set disappears"""
    pair([("sadd", "s", "a", "b", "a"), ("sadd", "s", "b"), ("smembers", "s"), ("sismember", "s", "a"), ("sismember", "s", "z"),
          ("scard", "s"), ("srem", "s", "a", "z"), ("srem", "s", "b"), ("exists", "s"), ("smembers", "nope")])


# ---- sorted sets ----

def test_zset_basics(pair):
    """Sorted sets | ZADD (mapping) / ZSCORE / ZCARD / ZRANGE / ZREVRANGE with scores / ZRANK"""
    pair([("zadd", "z", {"a": 10, "b": 5, "c": 7.5}), ("zadd", "z", {"a": 11}), ("zscore", "z", "a"), ("zscore", "z", "nope"),
          ("zcard", "z"), ("zrange", "z", 0, -1), ("zrange", "z", 0, 1, {"withscores": True}),
          ("zrevrange", "z", 0, -1, {"withscores": True}), ("zrank", "z", "c"), ("zrank", "z", "nope")])


def test_zset_options(pair):
    """Sorted sets | ZADD NX / XX / CH, ZINCRBY, ZREM, ZCOUNT, ZRANGEBYSCORE with bounds and LIMIT"""
    pair([("zadd", "z", {"a": 1, "b": 2}), ("zadd", "z", {"a": 5, "c": 3}, {"nx": True}), ("zscore", "z", "a"),
          ("zadd", "z", {"b": 9, "d": 4}, {"xx": True}), ("zcard", "z"), ("zadd", "z", {"b": 9, "c": 8}, {"ch": True}),
          ("zincrby", "z", 1.5, "a"), ("zincrby", "z", 2, "new"), ("zrem", "z", "new", "nope"), ("zcount", "z", 1, 8),
          ("zrangebyscore", "z", "-inf", "+inf"), ("zrangebyscore", "z", "(1", 8, {"withscores": True}),
          ("zrangebyscore", "z", 0, 100, {"start": 1, "num": 1})])


# ---- pipeline ----

def test_pipeline_batching(pair):
    """Pipeline | pipeline(transaction=False): queued commands, shaped replies, errors inside the batch"""
    def batch(conn):
        p = conn.pipeline(transaction=False)
        p.set("a", "1"); p.incr("a"); p.get("a"); p.hset("h", "f", "v"); p.hgetall("h"); p.sadd("s", "x"); p.smembers("s")
        return p.execute()
    real, ours = pair.conns
    expected = [True, 2, b"2", 1, {b"f": b"v"}, 1, {b"x"}]
    assert batch(ours) == expected
    if not pair.dry:
        assert batch(real) == expected
    # an error inside the batch raises on execute(); raise_on_error=False returns it in place
    p = ours.pipeline(transaction=False)
    p.set("t", "x"); p.incr("t"); p.get("t")
    with pytest.raises(Exception):
        p.execute()
    p.set("t", "x"); p.incr("t"); p.get("t")
    out = p.execute(raise_on_error=False)
    assert out[0] is True and isinstance(out[1], Exception) and out[2] == b"x"


# ---- errors ----

def test_wrongtype(pair):
    """Errors | WRONGTYPE: a hash command on a string key raises ResponseError"""
    pair([("set", "k", "v"), ("hset", "k", "f", "v"), ("lpush", "k", "v"), ("sadd", "k", "v"), ("zadd", "k", {"m": 1}), ("get", "k")])


def test_decode_responses(pair, ours_factory):
    """General | decode_responses=True returns str everywhere (get / hgetall / lrange / smembers / zrange)"""
    import os
    ours = ours_factory(decode_responses=True)
    conns = [ours]
    url = os.environ.get("REDIS_URL")
    if url:
        import redis as real_redis
        conns.insert(0, real_redis.Redis.from_url(url, decode_responses=True))
    results = []
    for c in conns:
        c.set("s", "v"); c.hset("h", "f", "v"); c.rpush("l", "a"); c.sadd("z", "m"); c.zadd("zs", {"m": 1})
        results.append((c.get("s"), c.hgetall("h"), c.lrange("l", 0, -1), c.smembers("z"), c.zrange("zs", 0, -1, withscores=True), c.type("s")))
    assert results[-1] == ("v", {"f": "v"}, ["a"], {"m"}, [("m", 1.0)], "string")
    assert len(set(map(repr, results))) == 1, results


# ---- unsupported by design: a real Redis does these, dynamoredis refuses with NotSupportedError ----

@pytest.mark.unsupported("pub/sub needs a server that holds connections")
def test_pubsub_refused(pair):
    """Unsupported | PUBLISH / SUBSCRIBE | needs connections held open"""
    pair.unsupported(("execute_command", "PUBLISH", "chan", "msg"))


@pytest.mark.unsupported("blocking commands need a server that holds connections")
def test_blocking_refused(pair):
    """Unsupported | BLPOP / BRPOP | needs connections held open"""
    pair([("rpush", "l", "x")])
    pair.unsupported(("execute_command", "BLPOP", "l", 1))


@pytest.mark.unsupported("Lua runs inside the server")
def test_lua_refused(pair):
    """Unsupported | EVAL | server-side scripting"""
    pair.unsupported(("execute_command", "EVAL", "return 1", 0))


@pytest.mark.unsupported("MULTI/EXEC and WATCH need a session")
def test_transaction_refused(pair):
    """Unsupported | pipeline(transaction=True) = MULTI/EXEC | needs a session; use transaction=False"""
    real, ours = pair.both(("pipeline",))
    assert ours.error == "NotSupportedError", ours
