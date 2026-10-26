"""Redis commands on one DynamoDB table.

Item layout (partition key `k`):
    k    the key                      t    string | hash | list | set | zset
    v    S or N (string; integers are stored as numbers so INCR is an atomic ADD),
         M (hash: field -> S|N), L (list), SS (set), M (zset: member -> N score)
    exp  expiry as epoch seconds; DynamoDB TTL deletes it eventually, reads treat it as gone immediately

Atomicity comes from DynamoDB itself: INCR / HINCRBY / ZINCRBY are ADD, LPUSH / RPUSH are list_append,
SADD / SREM are set ADD / DELETE, type safety is a ConditionExpression (-> WRONGTYPE). Commands that
need the previous value for their reply (HSET's "fields added", SADD's "members added", LPOP) read first.

Values are strings, as in Redis. Non-UTF-8 bytes travel as {"$b": base64} and are stored as S base64
with a marker prefix. Every call reports the consumed capacity units (cu) to the caller.
"""
import base64
import fnmatch
import re
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

_B = "\x00b64:"   # prefix marking a base64-stored binary value


class ResponseError(Exception):
    """A Redis error reply (WRONGTYPE, ERR value is not an integer, ...)."""


class NotSupported(Exception):
    pass


def _now():
    return int(time.time())


def _num(s):
    """Redis strings that are integers are stored as numbers: INCR needs that."""
    return int(s) if isinstance(s, str) and re.fullmatch(r"-?\d{1,18}", s) else s


def _text(v):
    if isinstance(v, Decimal):
        return str(int(v)) if v == v.to_integral_value() else str(v)
    return v


def _in(v):
    """Wire value -> stored value."""
    if isinstance(v, dict) and "$b" in v:
        return _B + v["$b"]
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return _num(str(v)) if isinstance(v, int) else str(v)
    return _num(v)


def _out(v):
    """Stored value -> wire value."""
    if isinstance(v, Decimal):
        return _text(v)
    if isinstance(v, str) and v.startswith(_B):
        return {"$b": v[len(_B):]}
    return v


def _int_arg(v, what="value"):
    try:
        return int(v)
    except (TypeError, ValueError):
        raise ResponseError(f"ERR {what} is not an integer or out of range") from None


class Engine:
    def __init__(self, table_name, client=None):
        self.table_name = table_name
        self.db = boto3.resource("dynamodb") if client is None else client
        self.table = self.db.Table(table_name)
        self.cu = 0.0   # consumed capacity units of the current command

    # ---------------- plumbing ----------------

    def _count(self, resp):
        cap = resp.get("ConsumedCapacity")
        if isinstance(cap, list):
            self.cu += sum(c.get("CapacityUnits", 0) for c in cap)
        elif cap:
            self.cu += cap.get("CapacityUnits", 0)
        return resp

    def _get(self, key):
        """The live item or None (expired items count as gone and are deleted)."""
        r = self._count(self.table.get_item(Key={"k": key}, ConsistentRead=True, ReturnConsumedCapacity="TOTAL"))
        item = r.get("Item")
        if item and "exp" in item and item["exp"] <= _now():
            self._delete(key)
            return None
        return item

    def _delete(self, key):
        r = self._count(self.table.delete_item(Key={"k": key}, ReturnValues="ALL_OLD", ReturnConsumedCapacity="TOTAL"))
        return "Attributes" in r

    def _update(self, key, expr, values=None, names=None, cond=None, cond_values=None):
        kw = {"Key": {"k": key}, "UpdateExpression": expr, "ReturnConsumedCapacity": "TOTAL", "ReturnValues": "ALL_NEW"}
        vals = dict(values or {}, **(cond_values or {}))
        if vals:
            kw["ExpressionAttributeValues"] = vals
        if names:
            kw["ExpressionAttributeNames"] = names
        if cond:
            kw["ConditionExpression"] = cond
        try:
            return self._count(self.table.update_item(**kw)).get("Attributes", {})
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                raise ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value") from None
            if code == "ValidationException" and "operand" in str(e).lower():
                raise ResponseError("ERR value is not an integer or out of range") from None
            raise

    def _typed(self, key, t):
        """A live item of type t, None if missing; WRONGTYPE if it is something else."""
        item = self._get(key)
        if item is not None and item["t"] != t:
            raise ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return item

    def _type_cond(self, t):
        return "attribute_not_exists(t) OR t = :t", {":t": t}

    # ---------------- dispatch ----------------

    def run(self, cmd, args):
        self.cu = 0.0
        fn = getattr(self, "cmd_" + cmd.lower(), None)
        if fn is None:
            raise NotSupported(cmd.upper())
        try:
            return fn(*args)
        except TypeError as e:
            if "positional argument" in str(e) or "required" in str(e):
                raise ResponseError(f"ERR wrong number of arguments for '{cmd.lower()}' command") from None
            raise

    # ---------------- strings ----------------

    def cmd_ping(self, *msg):
        return msg[0] if msg else "PONG"

    def cmd_set(self, key, value, *opts):
        ex, px, nx, xx, keepttl = None, None, False, False, False
        i = 0
        while i < len(opts):
            o = str(opts[i]).upper()
            if o == "EX":
                ex = _int_arg(opts[i + 1]); i += 2
            elif o == "PX":
                px = _int_arg(opts[i + 1]); i += 2
            elif o == "NX":
                nx = True; i += 1
            elif o == "XX":
                xx = True; i += 1
            elif o == "KEEPTTL":
                keepttl = True; i += 1
            else:
                raise ResponseError("ERR syntax error")
        item = {"k": key, "t": "string", "v": _in(value)}
        exp = (_now() + ex) if ex else (_now() + (px + 999) // 1000) if px else None
        if keepttl:
            old = self._get(key)
            if old and "exp" in old:
                exp = int(old["exp"])
        if exp:
            item["exp"] = exp
        kw = {"Item": item, "ReturnConsumedCapacity": "TOTAL"}
        if nx:
            kw["ConditionExpression"] = Attr("k").not_exists() | Attr("exp").lte(_now())
        elif xx:
            kw["ConditionExpression"] = Attr("k").exists() & (Attr("exp").not_exists() | Attr("exp").gt(_now()))
        try:
            self._count(self.table.put_item(**kw))
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return None
            raise
        return True

    def cmd_setex(self, key, seconds, value):
        return self.cmd_set(key, value, "EX", seconds)

    def cmd_psetex(self, key, ms, value):
        return self.cmd_set(key, value, "PX", ms)

    def cmd_setnx(self, key, value):
        return 1 if self.cmd_set(key, value, "NX") else 0

    def cmd_get(self, key):
        item = self._typed(key, "string")
        return None if item is None else _out(item["v"])

    def cmd_getdel(self, key):
        v = self.cmd_get(key)
        if v is not None:
            self._delete(key)
        return v

    def cmd_getset(self, key, value):
        old = self.cmd_get(key)
        self.cmd_set(key, value)
        return old

    def cmd_mget(self, *keys):
        return [self.cmd_get_or_none(k) for k in keys]

    def cmd_get_or_none(self, key):
        item = self._get(key)
        return _out(item["v"]) if item is not None and item["t"] == "string" else None

    def cmd_mset(self, *kv):
        if len(kv) % 2:
            raise ResponseError("ERR wrong number of arguments for 'mset' command")
        with self.table.batch_writer() as b:
            for i in range(0, len(kv), 2):
                b.put_item(Item={"k": kv[i], "t": "string", "v": _in(kv[i + 1])})
        self.cu += len(kv) // 2
        return True

    def cmd_strlen(self, key):
        v = self.cmd_get(key)
        return 0 if v is None else len(v["$b"] and base64.b64decode(v["$b"]) if isinstance(v, dict) else v.encode())

    def cmd_append(self, key, value):
        old = self.cmd_get(key) or ""
        new = (old if isinstance(old, str) else "") + str(value)
        self.cmd_set(key, new)
        return len(new.encode())

    def _incr(self, key, n):
        self._typed(key, "string")   # purge an expired item, enforce the type
        cond, cv = self._type_cond("string")
        attrs = self._update(key, "SET t = :t, v = if_not_exists(v, :z) + :n", {":z": 0, ":n": n}, cond=cond, cond_values=cv)
        return int(attrs["v"])

    def cmd_incr(self, key):
        return self._incr(key, 1)

    def cmd_decr(self, key):
        return self._incr(key, -1)

    def cmd_incrby(self, key, n):
        return self._incr(key, _int_arg(n))

    def cmd_decrby(self, key, n):
        return self._incr(key, -_int_arg(n))

    # ---------------- keys ----------------

    def cmd_del(self, *keys):
        return sum(1 for k in keys if self._get(k) is not None and self._delete(k))

    def cmd_unlink(self, *keys):
        return self.cmd_del(*keys)

    def cmd_exists(self, *keys):
        return sum(1 for k in keys if self._get(k) is not None)

    def cmd_type(self, key):
        item = self._get(key)
        return "none" if item is None else item["t"]

    def cmd_expire(self, key, seconds):
        return self._expire_at(key, _now() + _int_arg(seconds))

    def cmd_pexpire(self, key, ms):
        return self._expire_at(key, _now() + (_int_arg(ms) + 999) // 1000)

    def cmd_expireat(self, key, ts):
        return self._expire_at(key, _int_arg(ts))

    def _expire_at(self, key, ts):
        if self._get(key) is None:
            return 0
        try:
            self._update(key, "SET exp = :e", {":e": ts}, cond="attribute_exists(k)")
        except ResponseError:
            return 0
        return 1

    def cmd_ttl(self, key):
        item = self._get(key)
        if item is None:
            return -2
        if "exp" not in item:
            return -1
        return max(int(item["exp"]) - _now(), 0)

    def cmd_pttl(self, key):
        t = self.cmd_ttl(key)
        return t * 1000 if t > 0 else t

    def cmd_persist(self, key):
        item = self._get(key)
        if item is None or "exp" not in item:
            return 0
        self._update(key, "REMOVE exp", cond="attribute_exists(k)")
        return 1

    def cmd_rename(self, src, dst):
        item = self._get(src)
        if item is None:
            raise ResponseError("ERR no such key")
        item = dict(item, k=dst)
        self._count(self.table.put_item(Item=item, ReturnConsumedCapacity="TOTAL"))
        self._delete(src)
        return True

    def cmd_keys(self, pattern="*"):
        prefix = re.split(r"[*?\[]", pattern, maxsplit=1)[0]
        keys, kw = [], {"ProjectionExpression": "k, #e", "ExpressionAttributeNames": {"#e": "exp"},
                        "ReturnConsumedCapacity": "TOTAL"}
        if prefix:
            kw["FilterExpression"] = Key("k").begins_with(prefix)
        while True:
            r = self._count(self.table.scan(**kw))
            keys += [i["k"] for i in r.get("Items", []) if ("exp" not in i or i["exp"] > _now()) and fnmatch.fnmatchcase(i["k"], pattern)]
            if "LastEvaluatedKey" not in r:
                return sorted(keys)
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def cmd_scan(self, cursor="0", *opts):
        match, count = "*", 10
        for i in range(0, len(opts), 2):
            o = str(opts[i]).upper()
            if o == "MATCH":
                match = opts[i + 1]
            elif o == "COUNT":
                count = _int_arg(opts[i + 1])
        kw = {"ProjectionExpression": "k, #e", "ExpressionAttributeNames": {"#e": "exp"}, "Limit": count,
              "ReturnConsumedCapacity": "TOTAL"}
        if cursor not in ("0", 0, ""):
            kw["ExclusiveStartKey"] = {"k": base64.b64decode(str(cursor)).decode()}
        r = self._count(self.table.scan(**kw))
        keys = [i["k"] for i in r.get("Items", []) if ("exp" not in i or i["exp"] > _now()) and fnmatch.fnmatchcase(i["k"], match)]
        nxt = base64.b64encode(r["LastEvaluatedKey"]["k"].encode()).decode() if "LastEvaluatedKey" in r else "0"
        return [nxt, keys]

    def cmd_dbsize(self):
        n, kw = 0, {"Select": "COUNT", "ReturnConsumedCapacity": "TOTAL"}
        while True:
            r = self._count(self.table.scan(**kw))
            n += r["Count"]
            if "LastEvaluatedKey" not in r:
                return n
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def cmd_flushdb(self):
        keys = self.cmd_keys("*")
        with self.table.batch_writer() as b:
            for k in keys:
                b.delete_item(Key={"k": k})
        self.cu += len(keys)
        return True

    def cmd_flushall(self):
        return self.cmd_flushdb()

    # ---------------- hashes ----------------

    def _ensure(self, key, t, empty):
        """Create the container item if missing (type-checked); returns the live item or None."""
        item = self._typed(key, t)
        if item is None:
            cond, cv = self._type_cond(t)
            self._update(key, "SET t = :t, v = if_not_exists(v, :empty)", {":empty": empty}, cond=cond, cond_values=cv)
        return item

    @staticmethod
    def _fields(item):
        return dict(item["v"]) if item is not None and isinstance(item.get("v"), dict) else {}

    def cmd_hset(self, key, *fv):
        if len(fv) % 2 or not fv:
            raise ResponseError("ERR wrong number of arguments for 'hset' command")
        before = self._fields(self._ensure(key, "hash", {}))
        names = {f"#f{i}": fv[i * 2] for i in range(len(fv) // 2)}
        vals = {f":v{i}": _in(fv[i * 2 + 1]) for i in range(len(fv) // 2)}
        self._update(key, "SET " + ", ".join(f"v.#f{i} = :v{i}" for i in range(len(fv) // 2)), vals, names,
                     cond="t = :t", cond_values={":t": "hash"})
        return sum(1 for i in range(len(fv) // 2) if fv[i * 2] not in before)

    def cmd_hmset(self, key, *fv):
        self.cmd_hset(key, *fv)
        return True

    def cmd_hsetnx(self, key, field, value):
        if field in self._fields(self._typed(key, "hash")):
            return 0
        self.cmd_hset(key, field, value)
        return 1

    def cmd_hget(self, key, field):
        v = self._fields(self._typed(key, "hash")).get(field)
        return None if v is None else _out(v)

    def cmd_hmget(self, key, *fields):
        f = self._fields(self._typed(key, "hash"))
        return [None if x not in f else _out(f[x]) for x in fields]

    def cmd_hgetall(self, key):
        return {k: _out(v) for k, v in self._fields(self._typed(key, "hash")).items()}

    def cmd_hkeys(self, key):
        return sorted(self._fields(self._typed(key, "hash")))

    def cmd_hvals(self, key):
        f = self._fields(self._typed(key, "hash"))
        return [_out(f[k]) for k in sorted(f)]

    def cmd_hlen(self, key):
        return len(self._fields(self._typed(key, "hash")))

    def cmd_hexists(self, key, field):
        return 1 if field in self._fields(self._typed(key, "hash")) else 0

    def cmd_hdel(self, key, *fields):
        f = self._fields(self._typed(key, "hash"))
        gone = [x for x in fields if x in f]
        if not gone:
            return 0
        if len(gone) == len(f):
            self._delete(key)           # Redis removes an empty hash
            return len(gone)
        names = {f"#f{i}": x for i, x in enumerate(gone)}
        self._update(key, "REMOVE " + ", ".join(f"v.#f{i}" for i in range(len(gone))), names=names,
                     cond="t = :t", cond_values={":t": "hash"})
        return len(gone)

    def cmd_hincrby(self, key, field, n):
        self._ensure(key, "hash", {})
        try:
            attrs = self._update(key, "ADD v.#f :n", {":n": _int_arg(n, "increment")}, {"#f": field},
                                 cond="t = :t", cond_values={":t": "hash"})
        except ResponseError as e:
            if "WRONGTYPE" in str(e):
                raise
            raise ResponseError("ERR hash value is not an integer") from None
        return int(attrs["v"][field])

    # ---------------- lists ----------------

    @staticmethod
    def _list(item):
        return list(item["v"]) if item is not None and isinstance(item.get("v"), list) else []

    def _push(self, key, values, left, only_existing=False):
        item = self._typed(key, "list")
        if only_existing and item is None:
            return 0
        vals = [_in(v) for v in values]
        if left:
            vals = vals[::-1]
            expr = "SET t = :t, v = list_append(:vals, if_not_exists(v, :empty))"
        else:
            expr = "SET t = :t, v = list_append(if_not_exists(v, :empty), :vals)"
        cond, cv = self._type_cond("list")
        attrs = self._update(key, expr, {":vals": vals, ":empty": []}, cond=cond, cond_values=cv)
        return len(attrs["v"])

    def cmd_lpush(self, key, *values):
        return self._push(key, values, True)

    def cmd_rpush(self, key, *values):
        return self._push(key, values, False)

    def cmd_lpushx(self, key, *values):
        return self._push(key, values, True, only_existing=True)

    def cmd_rpushx(self, key, *values):
        return self._push(key, values, False, only_existing=True)

    def _pop(self, key, left, count=None):
        item = self._typed(key, "list")
        lst = self._list(item)
        if not lst:
            return None if count is None else None
        n = 1 if count is None else _int_arg(count)
        taken = lst[:n] if left else lst[-n:][::-1]
        if n >= len(lst):
            self._delete(key)
        else:
            idx = range(n) if left else range(len(lst) - n, len(lst))
            self._update(key, "REMOVE " + ", ".join(f"v[{i}]" for i in idx), cond="t = :t AND size(v) = :n",
                         cond_values={":t": "list", ":n": len(lst)})
        out = [_out(v) for v in taken]
        return out[0] if count is None else out

    def cmd_lpop(self, key, count=None):
        return self._pop(key, True, count)

    def cmd_rpop(self, key, count=None):
        return self._pop(key, False, count)

    def cmd_lrange(self, key, start, stop):
        lst = self._list(self._typed(key, "list"))
        start, stop = _int_arg(start), _int_arg(stop)
        n = len(lst)
        start = max(start + n, 0) if start < 0 else start
        stop = stop + n if stop < 0 else min(stop, n - 1)
        return [_out(v) for v in lst[start:stop + 1]] if start <= stop else []

    def cmd_llen(self, key):
        return len(self._list(self._typed(key, "list")))

    def cmd_lindex(self, key, index):
        lst = self._list(self._typed(key, "list"))
        i = _int_arg(index)
        return _out(lst[i]) if -len(lst) <= i < len(lst) else None

    def cmd_lset(self, key, index, value):
        item = self._typed(key, "list")
        if item is None:
            raise ResponseError("ERR no such key")
        lst = self._list(item)
        i = _int_arg(index)
        if not -len(lst) <= i < len(lst):
            raise ResponseError("ERR index out of range")
        self._update(key, f"SET v[{i % len(lst)}] = :x", {":x": _in(value)}, cond="t = :t", cond_values={":t": "list"})
        return True

    def cmd_ltrim(self, key, start, stop):
        lst = self._list(self._typed(key, "list"))
        if not lst:
            return True
        keep = self.cmd_lrange(key, start, stop)
        if not keep:
            self._delete(key)
        else:
            self._update(key, "SET v = :v", {":v": [_in(x) for x in keep]}, cond="t = :t", cond_values={":t": "list"})
        return True

    # ---------------- sets ----------------

    @staticmethod
    def _set(item):
        return set(item["v"]) if item is not None and isinstance(item.get("v"), (set, frozenset)) else set()

    def cmd_sadd(self, key, *members):
        before = self._set(self._typed(key, "set"))
        new = {str(m) for m in members} - before
        if not new:
            return 0
        cond, cv = self._type_cond("set")
        self._update(key, "ADD v :m SET t = :t", {":m": new}, cond=cond, cond_values=cv)
        return len(new)

    def cmd_srem(self, key, *members):
        have = self._set(self._typed(key, "set"))
        gone = {str(m) for m in members} & have
        if not gone:
            return 0
        if gone == have:
            self._delete(key)
        else:
            self._update(key, "DELETE v :m", {":m": gone}, cond="t = :t", cond_values={":t": "set"})
        return len(gone)

    def cmd_smembers(self, key):
        return sorted(self._set(self._typed(key, "set")))

    def cmd_sismember(self, key, member):
        return 1 if str(member) in self._set(self._typed(key, "set")) else 0

    def cmd_scard(self, key):
        return len(self._set(self._typed(key, "set")))

    # ---------------- sorted sets ----------------

    def _zitems(self, item):
        f = self._fields(item)
        return sorted(((float(s), m) for m, s in f.items()), key=lambda x: (x[0], x[1]))

    def cmd_zadd(self, key, *args):
        opts, i = {"NX": False, "XX": False, "CH": False, "INCR": False}, 0
        while i < len(args) and str(args[i]).upper() in opts:
            opts[str(args[i]).upper()] = True; i += 1
        pairs = args[i:]
        if len(pairs) % 2 or not pairs:
            raise ResponseError("ERR syntax error")
        if opts["INCR"]:
            return self.cmd_zincrby(key, pairs[0], pairs[1])
        before = self._fields(self._ensure(key, "zset", {}))
        names, vals, sets = {}, {}, []
        changed = added = 0
        for j in range(0, len(pairs), 2):
            score, member = pairs[j], str(pairs[j + 1])
            try:
                score = Decimal(str(float(score)))
            except ValueError:
                raise ResponseError("ERR value is not a valid float") from None
            if (opts["NX"] and member in before) or (opts["XX"] and member not in before):
                continue
            if member not in before:
                added += 1
            if before.get(member) != score:
                changed += 1
            names[f"#m{j}"] = member
            vals[f":s{j}"] = score
            sets.append(f"v.#m{j} = :s{j}")
        if sets:
            self._update(key, "SET " + ", ".join(sets), vals, names, cond="t = :t", cond_values={":t": "zset"})
        return changed if opts["CH"] else added

    def cmd_zincrby(self, key, increment, member):
        self._ensure(key, "zset", {})
        try:
            inc = Decimal(str(float(increment)))
        except ValueError:
            raise ResponseError("ERR value is not a valid float") from None
        attrs = self._update(key, "ADD v.#m :n", {":n": inc}, {"#m": str(member)}, cond="t = :t", cond_values={":t": "zset"})
        return _text(attrs["v"][str(member)])

    def cmd_zscore(self, key, member):
        f = self._fields(self._typed(key, "zset"))
        return None if str(member) not in f else _text(f[str(member)])

    def cmd_zcard(self, key):
        return len(self._fields(self._typed(key, "zset")))

    def cmd_zrem(self, key, *members):
        f = self._fields(self._typed(key, "zset"))
        gone = [str(m) for m in members if str(m) in f]
        if not gone:
            return 0
        if len(gone) == len(f):
            self._delete(key)
        else:
            names = {f"#m{i}": m for i, m in enumerate(gone)}
            self._update(key, "REMOVE " + ", ".join(f"v.#m{i}" for i in range(len(gone))), names=names,
                         cond="t = :t", cond_values={":t": "zset"})
        return len(gone)

    def _zrange(self, key, start, stop, withscores, reverse):
        items = self._zitems(self._typed(key, "zset"))
        if reverse:
            items = items[::-1]
        start, stop = _int_arg(start), _int_arg(stop)
        n = len(items)
        start = max(start + n, 0) if start < 0 else start
        stop = stop + n if stop < 0 else min(stop, n - 1)
        sel = items[start:stop + 1] if start <= stop else []
        return [[m, _text(Decimal(str(s)))] for s, m in sel] if withscores else [m for _s, m in sel]

    def cmd_zrange(self, key, start, stop, *opts):
        return self._zrange(key, start, stop, "WITHSCORES" in [str(o).upper() for o in opts], False)

    def cmd_zrevrange(self, key, start, stop, *opts):
        return self._zrange(key, start, stop, "WITHSCORES" in [str(o).upper() for o in opts], True)

    def cmd_zrangebyscore(self, key, lo, hi, *opts):
        def bound(x, default):
            x = str(x)
            if x in ("-inf", "+inf", "inf"):
                return float(x), True
            if x.startswith("("):
                return float(x[1:]), False
            return float(x), True
        lo_v, lo_inc = bound(lo, None)
        hi_v, hi_inc = bound(hi, None)
        items = [(s, m) for s, m in self._zitems(self._typed(key, "zset"))
                 if (s > lo_v or (lo_inc and s == lo_v)) and (s < hi_v or (hi_inc and s == hi_v))]
        up = [str(o).upper() for o in opts]
        if "LIMIT" in up:
            i = up.index("LIMIT")
            off, cnt = _int_arg(opts[i + 1]), _int_arg(opts[i + 2])
            items = items[off:off + cnt] if cnt >= 0 else items[off:]
        return [[m, _text(Decimal(str(s)))] for s, m in items] if "WITHSCORES" in up else [m for _s, m in items]

    def cmd_zrank(self, key, member):
        items = self._zitems(self._typed(key, "zset"))
        for i, (_s, m) in enumerate(items):
            if m == str(member):
                return i
        return None

    def cmd_zcount(self, key, lo, hi):
        return len(self.cmd_zrangebyscore(key, lo, hi))


# commands that need a real Redis server: refused explicitly, never imitated
UNSUPPORTED = {c: "pub/sub needs a server that holds connections" for c in ("SUBSCRIBE", "PSUBSCRIBE", "PUBLISH", "UNSUBSCRIBE")}
UNSUPPORTED.update({c: "blocking commands need a server that holds connections" for c in ("BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX")})
UNSUPPORTED.update({c: "Lua scripting runs inside the server" for c in ("EVAL", "EVALSHA", "SCRIPT", "FUNCTION")})
UNSUPPORTED.update({c: "streams are not implemented (use SQS-based projects of the series)" for c in ("XADD", "XREAD", "XRANGE", "XLEN", "XGROUP", "XREADGROUP", "XACK", "XDEL", "XTRIM")})
UNSUPPORTED.update({c: "optimistic transactions need a session" for c in ("WATCH", "UNWATCH", "MULTI", "EXEC", "DISCARD")})
UNSUPPORTED.update({c: "no server to configure" for c in ("CONFIG", "SAVE", "BGSAVE", "SHUTDOWN", "SLAVEOF", "REPLICAOF", "CLUSTER", "DEBUG", "MONITOR", "SYNC", "CLIENT")})
