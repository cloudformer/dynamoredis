"""Compatibility harness: the same redis-py calls on a real Redis (Docker) and on dynamoredis, compared.

Each step is (method name, args..., {kwargs}?) run through redis-py on the real server and through
dynamoredis.redis on the in-process Lambda (mocked DynamoDB); the return value (type included) or the
raised error (class name) must match. One test = one feature; its docstring is the report row.
"""
import base64
import json
import os
from dataclasses import dataclass

import pytest

import compat_report
import lambda_function as lf
from dynamoredis import redis as ours_redis

AUTH = "Basic " + base64.b64encode(b"admin:pw").decode()


@dataclass
class Outcome:
    value: object
    error: object = None


def run_step(conn, step):
    """A trailing dict is kwargs, except for commands whose last positional argument is a mapping
    (mset / hmset / zadd); zadd with two trailing dicts = (mapping, kwargs)."""
    name, args = step[0], list(step[1:])
    positional_map = name in ("mset", "hmset") or (name == "zadd" and not (len(args) >= 2 and isinstance(args[-2], dict)))
    kwargs = args.pop() if args and isinstance(args[-1], dict) and not positional_map else {}
    try:
        v = getattr(conn, name)(*args, **kwargs)
        if hasattr(v, "__next__"):   # generators (scan_iter)
            v = list(v)
        return Outcome(v)
    except Exception as e:  # noqa: BLE001
        return Outcome(None, type(e).__name__)


@pytest.fixture
def real():
    url = os.environ.get("REDIS_URL")
    if not url and os.environ.get("COMPAT_DRY"):
        return None
    if not url:
        pytest.skip("set REDIS_URL (see tests/compat/docker-compose.yml) to run against a real Redis")
    redis = pytest.importorskip("redis")
    conn = redis.Redis.from_url(url)
    conn.flushdb()
    if not compat_report.versions.get("reference"):
        compat_report.versions["reference"] = f"Redis {conn.info()['redis_version']} (Docker)"
    return conn


def in_process(monkeypatch, **kw):
    """dynamoredis.redis with its HTTP layer calling the Lambda handler in-process."""
    c = ours_redis.Redis(host="in-process.test", username="admin", password="pw", **kw)

    def post(path, payload):
        event = {"requestContext": {"http": {"method": "POST"}}, "rawPath": path,
                 "headers": {"authorization": AUTH}, "body": json.dumps(payload)}
        resp = lf.lambda_handler(event, None)
        data = json.loads(resp["body"])
        c.last = {"ms": data.get("ms"), "cu": data.get("cu")}
        if resp["statusCode"] >= 400:
            raise ours_redis._error(data)
        return data

    monkeypatch.setattr(c, "_post", post)
    return c


@pytest.fixture
def ours(table, monkeypatch):
    return in_process(monkeypatch)


@pytest.fixture
def ours_factory(table, monkeypatch):
    return lambda **kw: in_process(monkeypatch, **kw)


@pytest.fixture
def pair(real, ours):
    dry = real is None

    def both(step):
        o = run_step(ours, step)
        if dry:
            print(f"  {'ERR ' + str(o.error) if o.error else 'ok  ' + repr(o.value)[:70]:80s} {step}")
            return o, o
        return run_step(real, step), o

    def run(steps):
        for step in steps:
            r, o = both(step)
            if not dry:
                assert r == o, f"{step}\n  real: {r}\n  ours: {o}"
    run.both = both

    def unsupported(step):
        r, o = both(step)
        assert dry or r.error is None, f"the reference rejected it itself: {r.error}"
        assert o.error == "NotSupportedError", f"{step}\n  not refused: {o}"
    run.unsupported = unsupported
    run.conns = (real, ours)   # for tests that need the clients themselves (pipelines, options)
    run.dry = dry
    return run
