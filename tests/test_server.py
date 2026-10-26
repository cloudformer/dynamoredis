"""The server Lambda called in-process against a mocked DynamoDB: auth, routing, the engine's guarantees."""
import base64
import json
import time

import lambda_function as lf
from engine import Engine

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()


def call(method, path, body=None, auth=AUTH, qs=None):
    event = {"requestContext": {"http": {"method": method}}, "rawPath": path,
             "headers": {"authorization": auth} if auth else {}, "body": json.dumps(body) if body else None,
             "queryStringParameters": qs}
    resp = lf.lambda_handler(event, None)
    ct = resp["headers"]["content-type"]
    return resp["statusCode"], json.loads(resp["body"]) if ct.startswith("application/json") else resp["body"]


def cmd(c, *args):
    return call("POST", "/api/cmd", {"cmd": c, "args": list(args)})


def test_console_is_public_and_api_needs_auth(table):
    status, body = call("GET", "/", auth=None)
    assert status == 200 and "<html" in body
    assert cmd("PING")[0] == 200
    assert call("POST", "/api/cmd", {"cmd": "PING", "args": []}, auth=None)[0] == 401


def test_strings_and_atomic_counters(table):
    assert cmd("SET", "k", "v")[1]["result"] is True
    assert cmd("GET", "k")[1]["result"] == "v"
    assert cmd("GET", "nope")[1]["result"] is None
    assert cmd("INCR", "n")[1]["result"] == 1
    assert cmd("INCRBY", "n", 41)[1]["result"] == 42
    assert cmd("GET", "n")[1]["result"] == "42"            # numbers read back as Redis strings
    status, data = cmd("INCR", "k")
    assert status == 400 and data["kind"] == "ResponseError" and "not an integer" in data["error"]
    assert cmd("SET", "k", "x", "NX")[1]["result"] is None  # NX on an existing key
    assert cmd("SET", "new", "x", "XX")[1]["result"] is None
    assert cmd("SETNX", "k2", "1")[1]["result"] == 1 and cmd("SETNX", "k2", "2")[1]["result"] == 0


def test_expiry_is_honoured_on_read(table, monkeypatch):
    cmd("SET", "k", "v", "EX", 10)
    assert 0 < cmd("TTL", "k")[1]["result"] <= 10
    monkeypatch.setattr("engine._now", lambda: int(time.time()) + 11)
    assert cmd("GET", "k")[1]["result"] is None             # expired: gone before DynamoDB's TTL sweep
    assert cmd("EXISTS", "k")[1]["result"] == 0


def test_wrongtype_is_refused(table):
    cmd("SET", "s", "v")
    status, data = cmd("HSET", "s", "f", "v")
    assert status == 400 and data["error"].startswith("WRONGTYPE")
    status, data = cmd("LPUSH", "s", "v")
    assert status == 400 and data["error"].startswith("WRONGTYPE")


def test_hash_list_set_zset(table):
    assert cmd("HSET", "h", "a", "1", "b", "x")[1]["result"] == 2
    assert cmd("HSET", "h", "a", "2")[1]["result"] == 0
    assert cmd("HINCRBY", "h", "a", 5)[1]["result"] == 7
    assert cmd("HGETALL", "h")[1]["result"] == {"a": "7", "b": "x"}
    assert cmd("HDEL", "h", "a", "b")[1]["result"] == 2 and cmd("EXISTS", "h")[1]["result"] == 0
    assert cmd("LPUSH", "l", "a", "b")[1]["result"] == 2 and cmd("RPUSH", "l", "c")[1]["result"] == 3
    assert cmd("LRANGE", "l", 0, -1)[1]["result"] == ["b", "a", "c"]
    assert cmd("LPOP", "l")[1]["result"] == "b" and cmd("RPOP", "l")[1]["result"] == "c"
    assert cmd("SADD", "s", "x", "y", "x")[1]["result"] == 2 and cmd("SISMEMBER", "s", "y")[1]["result"] == 1
    assert cmd("SREM", "s", "x", "y")[1]["result"] == 2 and cmd("TYPE", "s")[1]["result"] == "none"
    assert cmd("ZADD", "z", 10, "a", 5, "b")[1]["result"] == 2
    assert cmd("ZRANGE", "z", 0, -1, "WITHSCORES")[1]["result"] == [["b", "5"], ["a", "10"]]
    assert cmd("ZINCRBY", "z", 2.5, "b")[1]["result"] == "7.5"


def test_keys_scan_pipeline_and_console_routes(table):
    for i in range(5):
        cmd("SET", f"user:{i}", i)
    cmd("SET", "other", "x")
    assert cmd("KEYS", "user:*")[1]["result"] == [f"user:{i}" for i in range(5)]
    assert cmd("DBSIZE")[1]["result"] == 6
    status, data = call("POST", "/api/pipeline", {"cmds": [["SET", "p", "1"], ["INCR", "p"], ["INCR", "other"], ["GET", "p"]]})
    assert status == 200
    assert data["results"][0] == {"result": True} and data["results"][1] == {"result": 2}
    assert data["results"][2]["kind"] == "ResponseError" and data["results"][3] == {"result": "2"}
    status, data = call("GET", "/api/keys", qs={"match": "user:*"})
    assert status == 200 and len(data["keys"]) == 5
    status, data = call("GET", "/api/key/user:1")
    assert status == 200 and data["type"] == "string" and data["value"] == "1" and data["ttl"] == -1
    assert call("GET", "/api/key/nope")[0] == 404


def test_unsupported_commands_are_refused(table):
    for c in ("SUBSCRIBE", "BLPOP", "EVAL", "MULTI", "XADD", "WATCH"):
        status, data = cmd(c, "x")
        assert status == 400 and data["kind"] == "NotSupportedError", c
    status, data = cmd("NOPE", "x")
    assert status == 400 and data["kind"] == "NotSupportedError"


# ---------------- concurrency: another client writes between our read and our write ----------------

def interleave(eng, monkeypatch, *cmds, every=False, at="write"):
    """Just before the engine's first write (or every write; at="read": just after its first read), another
    client runs cmds on the same table: the moment between "read and decide" and "write" where a concurrent
    change used to be lost. A callable in cmds runs as is (e.g. to move the clock)."""
    other = Engine(eng.table_name, client=eng.db)

    def run_others():
        ran.append(1)
        for c in cmds:
            c() if callable(c) else other.run(c[0], list(c[1:]))

    def hook(real, after):
        def call(*args, **kw):
            if after:
                r = real(*args, **kw)
            if every or not ran:
                run_others()
            return r if after else real(*args, **kw)
        return call
    ran = []
    if at == "read":   # after the engine has read the item and judged it (live, type, expiry)
        monkeypatch.setattr(eng, "_get", hook(eng._get, True))
        return ran
    for name in ("put_item", "update_item", "delete_item"):
        monkeypatch.setattr(eng.table, name, hook(getattr(eng.table, name), False))
    monkeypatch.setattr(eng.db.meta.client, "transact_write_items", hook(eng.db.meta.client.transact_write_items, False))
    return ran


def test_expired_cleanup_keeps_a_concurrent_set(table, monkeypatch):
    cmd("SET", "k", "old", "EX", 10)
    monkeypatch.setattr("engine._now", lambda: int(time.time()) + 11)
    interleave(table, monkeypatch, ("SET", "k", "new"))     # lands after GET saw the expired item
    assert cmd("GET", "k")[1]["result"] == "new"
    assert cmd("GET", "k")[1]["result"] == "new"


def test_getdel_is_one_atomic_delete(table, monkeypatch):
    cmd("SET", "k", "old")
    interleave(table, monkeypatch, ("SET", "k", "new"))
    got = cmd("GETDEL", "k")[1]["result"]
    assert "new" in (got, cmd("GET", "k")[1]["result"])     # the other client's value is returned or kept   # every value written is returned or kept
    cmd("HSET", "h", "f", "v")
    assert cmd("GETDEL", "h")[1]["error"].startswith("WRONGTYPE") and cmd("HLEN", "h")[1]["result"] == 1


def test_removing_the_last_member_keeps_a_concurrent_add(table, monkeypatch):
    cmd("HSET", "h", "a", "1")
    cmd("SADD", "s", "a")
    cmd("ZADD", "z", 1, "a")
    interleave(table, monkeypatch, ("HSET", "h", "b", "2"), ("SADD", "s", "b"), ("ZADD", "z", 2, "b"))
    assert cmd("HDEL", "h", "a")[1]["result"] == 1
    assert cmd("SREM", "s", "a")[1]["result"] == 1
    assert cmd("ZREM", "z", "a")[1]["result"] == 1
    assert cmd("HGETALL", "h")[1]["result"] == {"b": "2"}
    assert cmd("SMEMBERS", "s")[1]["result"] == ["b"]
    assert cmd("ZRANGE", "z", 0, -1)[1]["result"] == ["b"]


def test_pop_keeps_a_concurrent_push(table, monkeypatch):
    cmd("RPUSH", "l", "a")
    cmd("RPUSH", "r", "a")
    interleave(table, monkeypatch, ("RPUSH", "l", "b"), ("LPUSH", "r", "b"))
    assert cmd("LPOP", "l")[1]["result"] == "a" and cmd("LRANGE", "l", 0, -1)[1]["result"] == ["b"]
    assert cmd("RPOP", "r")[1]["result"] == "a" and cmd("LRANGE", "r", 0, -1)[1]["result"] == ["b"]


def test_pop_is_not_fooled_by_an_unchanged_length(table, monkeypatch):
    cmd("RPUSH", "l", "a", "b", "c")
    interleave(table, monkeypatch, ("LPOP", "l"), ("RPUSH", "l", "d"))   # same length, different list
    assert cmd("LPOP", "l")[1]["result"] == "b"             # "a" went to the other client, never twice
    assert cmd("LRANGE", "l", 0, -1)[1]["result"] == ["c", "d"]


def test_ltrim_keeps_a_concurrent_push(table, monkeypatch):
    cmd("RPUSH", "l", "a", "b", "c")
    interleave(table, monkeypatch, ("RPUSH", "l", "d"))
    assert cmd("LTRIM", "l", 1, -1)[1]["result"] is True
    assert cmd("LRANGE", "l", 0, -1)[1]["result"] == ["b", "c", "d"]


def test_rename_moves_the_latest_value_in_one_transaction(table, monkeypatch):
    cmd("SET", "a", "1", "EX", 100)
    interleave(table, monkeypatch, ("SET", "a", "2"))
    assert cmd("RENAME", "a", "b")[1]["result"] is True
    assert cmd("GET", "b")[1]["result"] == "2" and cmd("EXISTS", "a")[1]["result"] == 0
    assert cmd("TTL", "b")[1]["result"] == -1               # the value written last had no TTL
    assert cmd("RENAME", "b", "b")[1]["result"] is True and cmd("GET", "b")[1]["result"] == "2"


def test_gives_up_with_a_clear_error_after_retries(table, monkeypatch):
    cmd("RPUSH", "l", "a")
    interleave(table, monkeypatch, ("RPUSH", "l", "x"), every=True)   # the list changes before every attempt
    status, data = cmd("LPOP", "l")
    assert status == 400 and data["error"] == "ERR concurrent modification of key 'l', retried 3 times"
    assert cmd("LRANGE", "l", 0, -1)[1]["result"] == ["a", "x", "x", "x", "x"]   # nothing popped, nothing lost
