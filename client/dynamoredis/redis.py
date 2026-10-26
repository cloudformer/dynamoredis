"""dynamoredis.redis — the redis-py API (redis.Redis, from_url, pipeline, exceptions) over HTTP to the
dynamoredis Lambda URL. Standard library only.

    from dynamoredis import redis
    r = redis.Redis(host="xxx.lambda-url.us-east-2.on.aws", username="admin", password="...")
    r.set("k", "v", ex=60)
    r.get("k")                      # b"v"  (decode_responses=True -> "v")
    r.incr("hits")
    r.hset("user:1", mapping={"name": "alice", "age": 30})
    with r.pipeline(transaction=False) as p:
        p.set("a", 1); p.get("a")
        p.execute()                 # [True, b"1"]

Values are Redis strings: str / int / float / bytes in, bytes out (or str with decode_responses=True).
Errors are the redis-py classes: ResponseError (WRONGTYPE, ERR ...), ConnectionError, TimeoutError,
AuthenticationError; what needs a real server raises NotSupportedError (a ResponseError).

Retry policy: a request is re-sent once only when it provably never reached the server (an idle
keep-alive connection closed by the peer); a timeout raises TimeoutError with the result unknown.
"""
import base64
import http.client
import json
import socket
import urllib.parse

__version__ = "0.1.0"


# ---------------- exceptions (redis-py hierarchy) ----------------

class RedisError(Exception): pass
class ConnectionError(RedisError): pass          # noqa: A001
class TimeoutError(ConnectionError): pass        # noqa: A001
class AuthenticationError(ConnectionError): pass
class ResponseError(RedisError): pass
class DataError(RedisError): pass
class NotSupportedError(ResponseError):
    """Needs a real Redis server (pub/sub, blocking commands, Lua, MULTI/EXEC, streams)."""


class exceptions:  # redis.exceptions.X, as in redis-py
    RedisError, ConnectionError, TimeoutError = RedisError, ConnectionError, TimeoutError
    AuthenticationError, ResponseError, DataError, NotSupportedError = AuthenticationError, ResponseError, DataError, NotSupportedError


class _NotDelivered(Exception):
    def __init__(self, cause, reused):
        super().__init__(cause)
        self.reused = reused


# ---------------- value encoding ----------------

def _arg(v):
    """Python value -> wire value (Redis strings; bytes that are not UTF-8 travel as {"$b": base64})."""
    if isinstance(v, bool):   # redis-py refuses bool too
        raise DataError("Invalid input of type: 'bool'. Convert to a bytes, string, int or float first.")
    if isinstance(v, (int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return {"$b": base64.b64encode(b).decode()}
    raise DataError(f"Invalid input of type: '{type(v).__name__}'. Convert to a bytes, string, int or float first.")


def _bytes(v):
    if isinstance(v, dict) and "$b" in v:
        return base64.b64decode(v["$b"])
    return str(v).encode()


class Redis:
    def __init__(self, host="localhost", port=None, db=0, password=None, username=None,
                 decode_responses=False, socket_timeout=30, **_ignored):
        """Same signature as redis.Redis; host is the Lambda URL (with or without https://)."""
        url = host if host.startswith("http") else "https://" + host
        p = urllib.parse.urlparse(url.rstrip("/"))
        self._host, self._base_path = p.netloc, p.path
        self._auth = base64.b64encode(f"{username or ''}:{password or ''}".encode()).decode()
        self.decode_responses = decode_responses
        self.timeout = socket_timeout or 30
        self._conn = None
        self.last = {}   # {"ms": server time, "cu": DynamoDB capacity units} of the last call

    @classmethod
    def from_url(cls, url, **kw):
        """redis://user:password@host/0 or https://user:password@host/ — the host is the Lambda URL."""
        p = urllib.parse.urlparse(url)
        return cls(host="https://" + p.hostname + (p.path if p.scheme.startswith("http") else ""),
                   username=p.username, password=p.password, **kw)

    # ---- transport ----

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        finally:
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def _send(self, path, body):
        reused = self._conn is not None
        if self._conn is None:
            self._conn = http.client.HTTPSConnection(self._host, timeout=self.timeout)
        headers = {"content-type": "application/json", "authorization": f"Basic {self._auth}"}
        try:
            self._conn.request("POST", self._base_path + path, body, headers)
        except (socket.timeout, TimeoutError):
            raise
        except (OSError, http.client.HTTPException) as e:
            raise _NotDelivered(e, reused) from None
        try:
            resp = self._conn.getresponse()
        except (http.client.RemoteDisconnected, http.client.BadStatusLine) as e:
            raise _NotDelivered(e, reused) from None
        raw = resp.read()
        try:
            data = json.loads(raw)
        except ValueError:
            data = {"error": raw[:200].decode(errors="replace")}
        return resp.status, data

    def _post(self, path, payload):
        body = json.dumps(payload)
        try:
            status, data = self._send(path, body)
        except _NotDelivered as e:
            self.close()
            if not e.reused:
                raise ConnectionError(f"Error connecting to {self._host}: {e}") from None
            status, data = self._send(path, body)
        except (socket.timeout, TimeoutError) as e:
            self.close()
            raise TimeoutError(f"Timeout talking to {self._host}; result unknown ({e})") from None
        self.last = {"ms": data.get("ms"), "cu": data.get("cu")}
        if status == 401:
            raise AuthenticationError("invalid username-password pair")
        if status >= 400:
            raise _error(data)
        return data

    def execute_command(self, cmd, *args):
        """Run any command; the reply is the raw wire value (str / int / list / dict / None)."""
        return self._post("/api/cmd", {"cmd": cmd, "args": [_arg(a) for a in args]})["result"]

    # ---- result shaping ----

    def _s(self, v):
        """A Redis string reply."""
        if v is None:
            return None
        b = _bytes(v)
        return b.decode() if self.decode_responses else b

    def _ls(self, v):
        return [self._s(x) for x in v]

    # ---- strings ----

    def ping(self):
        return self.execute_command("PING") == "PONG"

    def set(self, name, value, ex=None, px=None, nx=False, xx=False, keepttl=False, get=False):
        if get:
            raise NotSupportedError("SET ... GET is not supported; use getset()")
        opts = []
        if ex is not None:
            opts += ["EX", int(ex.total_seconds()) if hasattr(ex, "total_seconds") else int(ex)]
        if px is not None:
            opts += ["PX", int(px.total_seconds() * 1000) if hasattr(px, "total_seconds") else int(px)]
        if nx:
            opts.append("NX")
        if xx:
            opts.append("XX")
        if keepttl:
            opts.append("KEEPTTL")
        return self.execute_command("SET", name, value, *opts)

    def setex(self, name, time, value):
        return self.set(name, value, ex=time)

    def psetex(self, name, time_ms, value):
        return self.set(name, value, px=time_ms)

    def setnx(self, name, value):
        return bool(self.execute_command("SETNX", name, value))

    def get(self, name):
        return self._s(self.execute_command("GET", name))

    def getdel(self, name):
        return self._s(self.execute_command("GETDEL", name))

    def getset(self, name, value):
        return self._s(self.execute_command("GETSET", name, value))

    def mget(self, keys, *args):
        keys = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._ls(self.execute_command("MGET", *keys, *args))

    def mset(self, mapping):
        flat = [x for k, v in mapping.items() for x in (k, v)]
        return self.execute_command("MSET", *flat) is True

    def strlen(self, name):
        return self.execute_command("STRLEN", name)

    def append(self, key, value):
        return self.execute_command("APPEND", key, value)

    def incr(self, name, amount=1):
        return self.execute_command("INCRBY", name, amount)

    def incrby(self, name, amount=1):
        return self.execute_command("INCRBY", name, amount)

    def decr(self, name, amount=1):
        return self.execute_command("DECRBY", name, amount)

    def decrby(self, name, amount=1):
        return self.execute_command("DECRBY", name, amount)

    # ---- keys ----

    def delete(self, *names):
        return self.execute_command("DEL", *names)

    def unlink(self, *names):
        return self.execute_command("DEL", *names)

    def exists(self, *names):
        return self.execute_command("EXISTS", *names)

    def expire(self, name, time):
        secs = int(time.total_seconds()) if hasattr(time, "total_seconds") else int(time)
        return bool(self.execute_command("EXPIRE", name, secs))

    def pexpire(self, name, time):
        ms = int(time.total_seconds() * 1000) if hasattr(time, "total_seconds") else int(time)
        return bool(self.execute_command("PEXPIRE", name, ms))

    def expireat(self, name, when):
        ts = int(when.timestamp()) if hasattr(when, "timestamp") else int(when)
        return bool(self.execute_command("EXPIREAT", name, ts))

    def ttl(self, name):
        return self.execute_command("TTL", name)

    def pttl(self, name):
        return self.execute_command("PTTL", name)

    def persist(self, name):
        return bool(self.execute_command("PERSIST", name))

    def type(self, name):
        return self._s(self.execute_command("TYPE", name))

    def rename(self, src, dst):
        return self.execute_command("RENAME", src, dst)

    def keys(self, pattern="*"):
        return self._ls(self.execute_command("KEYS", pattern))

    def scan(self, cursor=0, match=None, count=None):
        opts = []
        if match is not None:
            opts += ["MATCH", match]
        if count is not None:
            opts += ["COUNT", count]
        nxt, found = self.execute_command("SCAN", cursor, *opts)
        return (0 if nxt == "0" else nxt), self._ls(found)

    def scan_iter(self, match=None, count=None):
        cursor = 0
        while True:
            cursor, found = self.scan(cursor, match=match, count=count)
            yield from found
            if cursor == 0:
                return

    def dbsize(self):
        return self.execute_command("DBSIZE")

    def flushdb(self, asynchronous=False):
        return self.execute_command("FLUSHDB")

    def flushall(self, asynchronous=False):
        return self.execute_command("FLUSHDB")

    # ---- hashes ----

    def hset(self, name, key=None, value=None, mapping=None, items=None):
        flat = []
        if key is not None:
            flat += [key, value]
        if mapping:
            flat += [x for k, v in mapping.items() for x in (k, v)]
        if items:
            flat += list(items)
        if not flat:
            raise DataError("'hset' with no key value pairs")
        return self.execute_command("HSET", name, *flat)

    def hmset(self, name, mapping):
        self.hset(name, mapping=mapping)
        return True

    def hsetnx(self, name, key, value):
        return bool(self.execute_command("HSETNX", name, key, value))

    def hget(self, name, key):
        return self._s(self.execute_command("HGET", name, key))

    def hmget(self, name, keys, *args):
        keys = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._ls(self.execute_command("HMGET", name, *keys, *args))

    def hgetall(self, name):
        return {self._s(k): self._s(v) for k, v in self.execute_command("HGETALL", name).items()}

    def hkeys(self, name):
        return self._ls(self.execute_command("HKEYS", name))

    def hvals(self, name):
        return self._ls(self.execute_command("HVALS", name))

    def hlen(self, name):
        return self.execute_command("HLEN", name)

    def hexists(self, name, key):
        return bool(self.execute_command("HEXISTS", name, key))

    def hdel(self, name, *keys):
        return self.execute_command("HDEL", name, *keys)

    def hincrby(self, name, key, amount=1):
        return self.execute_command("HINCRBY", name, key, amount)

    # ---- lists ----

    def lpush(self, name, *values):
        return self.execute_command("LPUSH", name, *values)

    def rpush(self, name, *values):
        return self.execute_command("RPUSH", name, *values)

    def lpushx(self, name, *values):
        return self.execute_command("LPUSHX", name, *values)

    def rpushx(self, name, *values):
        return self.execute_command("RPUSHX", name, *values)

    def lpop(self, name, count=None):
        r = self.execute_command("LPOP", name) if count is None else self.execute_command("LPOP", name, count)
        return self._s(r) if count is None else (None if r is None else self._ls(r))

    def rpop(self, name, count=None):
        r = self.execute_command("RPOP", name) if count is None else self.execute_command("RPOP", name, count)
        return self._s(r) if count is None else (None if r is None else self._ls(r))

    def lrange(self, name, start, end):
        return self._ls(self.execute_command("LRANGE", name, start, end))

    def llen(self, name):
        return self.execute_command("LLEN", name)

    def lindex(self, name, index):
        return self._s(self.execute_command("LINDEX", name, index))

    def lset(self, name, index, value):
        return self.execute_command("LSET", name, index, value)

    def ltrim(self, name, start, end):
        return self.execute_command("LTRIM", name, start, end)

    # ---- sets ----

    def sadd(self, name, *values):
        return self.execute_command("SADD", name, *values)

    def srem(self, name, *values):
        return self.execute_command("SREM", name, *values)

    def smembers(self, name):
        return set(self._ls(self.execute_command("SMEMBERS", name)))

    def sismember(self, name, value):
        return bool(self.execute_command("SISMEMBER", name, value))

    def scard(self, name):
        return self.execute_command("SCARD", name)

    # ---- sorted sets ----

    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False):
        opts = [o for o, on in (("NX", nx), ("XX", xx), ("CH", ch), ("INCR", incr)) if on]
        flat = [x for member, score in mapping.items() for x in (score, member)]
        r = self.execute_command("ZADD", name, *opts, *flat)
        return float(r) if incr and r is not None else r

    def zincrby(self, name, amount, value):
        return float(self.execute_command("ZINCRBY", name, amount, value))

    def zscore(self, name, value):
        r = self.execute_command("ZSCORE", name, value)
        return None if r is None else float(r)

    def zcard(self, name):
        return self.execute_command("ZCARD", name)

    def zrem(self, name, *values):
        return self.execute_command("ZREM", name, *values)

    def zrank(self, name, value):
        return self.execute_command("ZRANK", name, value)

    def zcount(self, name, min, max):
        return self.execute_command("ZCOUNT", name, min, max)

    def _zpairs(self, r, withscores, score_cast_func):
        if withscores:
            return [(self._s(m), score_cast_func(s)) for m, s in r]
        return self._ls(r)

    def zrange(self, name, start, end, desc=False, withscores=False, score_cast_func=float):
        cmd = "ZREVRANGE" if desc else "ZRANGE"
        r = self.execute_command(cmd, name, start, end, *(["WITHSCORES"] if withscores else []))
        return self._zpairs(r, withscores, score_cast_func)

    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        return self.zrange(name, start, end, desc=True, withscores=withscores, score_cast_func=score_cast_func)

    def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False, score_cast_func=float):
        opts = ["WITHSCORES"] if withscores else []
        if start is not None and num is not None:
            opts += ["LIMIT", start, num]
        r = self.execute_command("ZRANGEBYSCORE", name, min, max, *opts)
        return self._zpairs(r, withscores, score_cast_func)

    # ---- pipeline ----

    def pipeline(self, transaction=True, shard_hint=None):
        if transaction:
            raise NotSupportedError("MULTI/EXEC needs a server session; use pipeline(transaction=False) for batching")
        return Pipeline(self)


class _Captured(Exception):
    def __init__(self, cmd):
        self.cmd = cmd


class _Recorder(Redis):
    """A Redis whose execute_command captures the command instead of sending it."""
    def __init__(self):
        pass

    def execute_command(self, cmd, *args):
        raise _Captured([cmd, *[_arg(a) for a in args]])


_RECORDER = _Recorder()


class Pipeline:
    """Queues commands (the same methods as Redis, each returns the pipeline) and sends them in one
    request on execute(); replies are shaped like the single-call methods would shape them."""

    def __init__(self, parent):
        self._parent = parent
        self._queue = []

    def __getattr__(self, name):
        method = getattr(Redis, name, None)
        if method is None or name.startswith("_") or name in ("pipeline", "execute", "close"):
            raise AttributeError(name)

        def queue(*args, **kwargs):
            try:
                method(_RECORDER, *args, **kwargs)
            except _Captured as c:
                self._queue.append(c.cmd)
            else:
                raise NotSupportedError(f"{name} cannot be pipelined")
            return self
        return queue

    def execute_command(self, cmd, *args):
        self._queue.append([cmd, *[_arg(a) for a in args]])
        return self

    def __len__(self):
        return len(self._queue)

    def reset(self):
        self._queue = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.reset()

    def execute(self, raise_on_error=True):
        if not self._queue:
            return []
        cmds, self._queue = self._queue, []
        data = self._parent._post("/api/pipeline", {"cmds": cmds})
        out = []
        for c, r in zip(cmds, data["results"]):
            if "error" in r:
                err = _error(r)
                if raise_on_error:
                    raise err
                out.append(err)
            else:
                out.append(_shape(self._parent, c[0], r["result"]))
        return out


def _shape(client, cmd, result):
    """Shape a raw reply the way the single-call method would (bytes / sets / tuples / bools)."""
    c = cmd.upper()
    if c in ("GET", "GETDEL", "GETSET", "HGET", "LPOP", "RPOP", "LINDEX", "TYPE"):
        return client._s(result) if not isinstance(result, list) else client._ls(result)
    if c in ("MGET", "HMGET", "HKEYS", "HVALS", "LRANGE", "KEYS", "SMEMBERS"):
        return set(client._ls(result)) if c == "SMEMBERS" else client._ls(result)
    if c == "HGETALL":
        return {client._s(k): client._s(v) for k, v in result.items()}
    if c in ("SETNX", "EXPIRE", "PEXPIRE", "EXPIREAT", "PERSIST", "HEXISTS", "HSETNX", "SISMEMBER"):
        return bool(result)
    if c in ("ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE"):
        return [(client._s(m), float(s)) for m, s in result] if result and isinstance(result[0], list) else client._ls(result)
    if c in ("ZSCORE", "ZINCRBY"):
        return None if result is None else float(result)
    if c == "PING":
        return result == "PONG"
    return result


def _error(data):
    kind, msg = data.get("kind"), data.get("error", "")
    if kind == "NotSupportedError":
        return NotSupportedError(msg)
    if kind == "ResponseError":
        return ResponseError(msg)
    return ConnectionError(msg)


def from_url(url, **kw):
    return Redis.from_url(url, **kw)


StrictRedis = Redis
