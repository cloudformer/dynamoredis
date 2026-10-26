"""dynamoredis server: one Lambda behind a Function URL — Redis commands over HTTP for the driver, a
RedisInsight-style console at GET /. Data lives in one DynamoDB table (engine.py).

    GET  /                       console (index.html)
    POST /api/cmd                {"cmd": "SET", "args": ["k", "v"]}        -> {"result": ..., "cu": 1.0, "ms": ...}
    POST /api/pipeline           {"cmds": [["SET","k","v"], ["GET","k"]]}  -> {"results": [{"result"} | {"error"}, ...]}
    GET  /api/keys?match=&cursor=&count=   console: a page of keys
    GET  /api/key/{key}          console: type, ttl, value

Errors: a Redis error reply is 400 {"error": "WRONGTYPE ...", "kind": "ResponseError"}; an unsupported
command is 400 with kind "NotSupportedError"; everything else 500.
"""
import base64
import hmac
import json
import os
import re
import time
import urllib.parse

from engine import UNSUPPORTED, Engine, NotSupported, ResponseError

USER = os.environ.get("USER_NAME", "")
PASSWORD = os.environ.get("PASSWORD", "")
CONSOLE = os.environ.get("CONSOLE", "1") == "1"
TABLE = os.environ.get("TABLE", "dynamoredis")

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
    INDEX_HTML = f.read()

_engine = None


def engine():
    global _engine
    if _engine is None:
        _engine = Engine(TABLE)
    return _engine


class BadRequest(Exception):
    pass


# ---------------- shared: the same in every project ----------------

def _json(status, data):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(data, ensure_ascii=False, default=str)}


def _body(event):
    raw = event.get("body") or "{}"
    try:
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw)
        data = json.loads(raw)
    except ValueError:
        raise BadRequest("request body is not valid JSON") from None
    if not isinstance(data, dict):
        raise BadRequest("request body must be a JSON object")
    return data


def _authed(event):
    if not USER:
        return True
    auth = (event.get("headers") or {}).get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    try:
        user, _, password = base64.b64decode(auth.split(None, 1)[1]).decode().partition(":")
    except Exception:
        return False
    return hmac.compare_digest(user.encode(), USER.encode()) & hmac.compare_digest(password.encode(), PASSWORD.encode())


def lambda_handler(event, context):
    http = (event.get("requestContext") or {}).get("http") or {}
    method, path = http.get("method", "GET"), event.get("rawPath", "/")
    if CONSOLE and method == "GET" and path in ("/", "/index.html"):
        return {"statusCode": 200, "headers": {"content-type": "text/html; charset=utf-8"}, "body": INDEX_HTML}
    if not _authed(event):
        return _json(401, {"error": "bad credentials"})
    t0 = time.perf_counter()
    status, data = 404, {"error": f"no route: {method} {path}"}
    try:
        for route_method, pattern, handler in ROUTES:
            match = pattern.fullmatch(path)
            if match and route_method == method:
                out = handler(event, **match.groupdict())
                status, data = out if isinstance(out, tuple) else (200, out)
                break
    except BadRequest as e:
        status, data = 400, {"error": str(e)}
    except ResponseError as e:
        status, data = 400, {"error": str(e), "kind": "ResponseError"}
    except NotSupported as e:
        status, data = 400, {"error": f"{e} is not supported: {UNSUPPORTED.get(str(e), 'not implemented')}", "kind": "NotSupportedError"}
    except Exception as e:  # noqa: BLE001
        status, data = 500, {"error": f"{type(e).__name__}: {e}"}
    data.setdefault("ms", round((time.perf_counter() - t0) * 1000, 1))
    return _json(status, data)


# ---------------- the API ----------------

def _run(cmd, args):
    if not isinstance(cmd, str) or not isinstance(args, list):
        raise BadRequest("cmd must be a string and args a list")
    if cmd.upper() in UNSUPPORTED:
        raise NotSupported(cmd.upper())
    e = engine()
    result = e.run(cmd, args)
    return {"result": result, "cu": round(e.cu, 1)}


def cmd(event):
    body = _body(event)
    return _run(body.get("cmd", ""), body.get("args", []))


def pipeline(event):
    cmds = _body(event).get("cmds")
    if not isinstance(cmds, list) or not cmds:
        raise BadRequest("cmds must be a non-empty list of [cmd, arg, ...]")
    results, cu = [], 0.0
    for c in cmds:
        if not isinstance(c, list) or not c:
            raise BadRequest("each command is [cmd, arg, ...]")
        try:
            r = _run(c[0], c[1:])
            cu += r["cu"]
            results.append({"result": r["result"]})
        except ResponseError as err:
            results.append({"error": str(err), "kind": "ResponseError"})
        except NotSupported as err:
            results.append({"error": f"{err} is not supported", "kind": "NotSupportedError"})
    return {"results": results, "cu": round(cu, 1)}


def keys(event):
    qs = event.get("queryStringParameters") or {}
    e = engine()
    cursor, found = e.run("scan", [qs.get("cursor", "0"), "MATCH", qs.get("match") or "*", "COUNT", qs.get("count", "100")])
    return {"keys": found, "cursor": cursor, "cu": round(e.cu, 1)}


def key(event, key):
    key = urllib.parse.unquote(key)
    e = engine()
    t = e.run("type", [key])
    if t == "none":
        return 404, {"error": "no such key"}
    ttl = e.run("ttl", [key])
    value = {"string": lambda: e.run("get", [key]), "hash": lambda: e.run("hgetall", [key]),
             "list": lambda: e.run("lrange", [key, 0, -1]), "set": lambda: e.run("smembers", [key]),
             "zset": lambda: e.run("zrange", [key, 0, -1, "WITHSCORES"])}[t]()
    return {"key": key, "type": t, "ttl": ttl, "value": value, "cu": round(e.cu, 1)}


ROUTES = [
    ("POST", re.compile(r"/api/cmd$"), cmd),
    ("POST", re.compile(r"/api/pipeline$"), pipeline),
    ("GET",  re.compile(r"/api/keys$"), keys),
    ("GET",  re.compile(r"/api/key/(?P<key>[^/]+)$"), key),
]
