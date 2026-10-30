# dynamoredis — a truly serverless Redis

> **ElastiCache / Redis** bills by the hour while idle. **dynamoredis** does the same job on **one DynamoDB
> table behind a Lambda Function URL** and costs ≈ $0 when nothing happens.
> Same client API (redis-py); when you outgrow it, go back to a real Redis by changing one import line.

[中文](README.zh.md) · [compatibility report](tests/compat/REPORT.md) · [tasks](tests/compat/TASKS.md) · [changelog](CHANGELOG.md)

![architecture](docs/architecture.svg)

| | ElastiCache Serverless (Redis) | dynamoredis |
|---|---|---|
| Idle cost / month | ~$90 — billed per GB-hour with a 1 GB minimum ($0.125 × 24 × 30), ECPUs on top | ≈ $0 |
| Endpoint | host:6379, RESP protocol, VPC | Lambda Function URL, HTTPS, anywhere |
| Storage | memory on nodes | one DynamoDB table (pay per request, TTL, multi-AZ, durable) |
| Client | `import redis` | `from dynamoredis import redis` |
| Console | RedisInsight | built-in web console (Lambda URL) |
| Client compatibility | — | [see the report](tests/compat/REPORT.md) |

## Why

Most Redis use is a cache, a few counters, sessions, a small queue — bursts of activity and long idle
stretches, for which ElastiCache Serverless still charges every hour for at least 1 GB (≈ $90 a month before a single request). DynamoDB already is a serverless key-value store
with atomic counters, sets, lists and TTL; what is missing is the Redis API. dynamoredis adds it: a Lambda
translates redis commands into DynamoDB calls, the driver speaks redis-py.

## How it works

Your code calls the Lambda Function URL with HTTP Basic auth (the service's user / password); the Lambda
runs each command against one DynamoDB table — item = key, type, value, optional expiry. Where Redis is
atomic, DynamoDB is atomic: `INCR` is an `ADD`, `LPUSH` is `list_append`, `SADD` is a set `ADD`, a type
mismatch is a condition failure (`WRONGTYPE`). Expiry is honoured on read at once; DynamoDB's TTL sweep
deletes the item later. Durable the moment a command returns, multi-AZ, no node to run out of memory.

## Quick start

### 1. Deploy (pick one)

**Terraform**

```bash
cp terraform/example.json terraform/my.json      # set aws_profile, env, console_auth_username, console_auth_password
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=my.json
terraform -chdir=terraform output                 # console_url
terraform -chdir=terraform destroy -var-file=my.json   # tear everything down (the table included)
```

**CloudFormation** (AWS CLI only)

```bash
cp cloudformation/example-params.json cloudformation/params.json
aws cloudformation package --template-file cloudformation/template.yaml \
    --s3-bucket <any-bucket-you-own> --output-template-file /tmp/dynamoredis.yaml
aws cloudformation deploy --template-file /tmp/dynamoredis.yaml --stack-name dynamoredis \
    --capabilities CAPABILITY_IAM --parameter-overrides file://cloudformation/params.json
```

Both create one DynamoDB table (`<name>-<env>`, pay per request, TTL on `exp`), one Lambda with a public
Function URL, and one scoped IAM role.

### 2. Open the console

Open `console_url` in a browser, sign in with `user` / `password`. Keys by prefix on the left (SCAN), the
value on the right by type (string, hash, list, set, sorted set) with its TTL, a command line on top —
`SET greeting hello`, `HGETALL user:1`, `ZADD board 10 alice`. Every call shows the server time and the
DynamoDB capacity units it consumed.

Paste this into the command box to get a small dataset to click around (one command per line; `FLUSHDB` clears it):

```
HSET user:tom name Tom age 31 city Boston
HSET user:mary name Mary age 28 city Seattle
HSET user:alice name Alice age 30 city Austin
SET user:tom:last_login 2026-10-26T05:00:00Z
INCR user:tom:visits
SADD user:tom:roles admin editor
ZADD user:leaderboard 120 tom 95 mary 150 alice
RPUSH user:tom:inbox "hello tom" "meeting at 3"
SET config:feature:dark_mode on
SETEX session:abc123 3600 tom
INCR counter:pageviews
LPUSH jobs:pending job-1 job-2
```

Then look at the DynamoDB table in the AWS console: one item per key — `t` is the type, `v` the value
(hash = Map, list = List, set = String Set, sorted set = Map of member → score, counters = Number),
`session:abc123` carries `exp`.

### 3. Connect from code

```bash
pip install dynamoredis
```

```python
from dynamoredis import redis        # later: `import redis` against a real Redis, nothing else changes

r = redis.Redis(host="xxx.lambda-url.us-east-2.on.aws", username="admin", password="...")
r.set("greeting", "hello", ex=60)
r.get("greeting")                    # b"hello"   (decode_responses=True -> "hello")
r.incr("hits")                       # atomic
r.hset("user:1", mapping={"name": "alice", "age": 30}); r.hgetall("user:1")
r.rpush("queue", "job-1", "job-2"); r.lpop("queue")
r.zadd("board", {"alice": 10, "bob": 20}); r.zrange("board", 0, -1, withscores=True)
with r.pipeline(transaction=False) as p:   # one HTTP request for the batch
    p.set("a", 1); p.get("a"); p.execute()
```

## Compatibility

[tests/compat/REPORT.md](tests/compat/REPORT.md) is regenerated by every test run: the same redis-py calls
run on a real Redis 7 (Docker) and on dynamoredis, comparing replies (type included) and errors.

| Status | Meaning |
|---|---|
| ✅ verified | identical to redis-py against Redis 7; safe to rely on |
| ❌ gap | known difference; the note names the task in [TASKS.md](tests/compat/TASKS.md) that closes it |
| ⚪ quirk | documented behaviour of Redis we deliberately do not imitate |
| ⛔ unsupported | refused with `NotSupportedError` — never silently different |

Run it yourself — the same commands CI runs on every push:

```bash
python3 -m pip install pytest boto3 moto redis
python3 -m pytest tests -q                 # leaves Docker alone: live if Redis is running, else the compat tests are skipped
python3 -m pytest tests -q --docker up     # starts Redis in Docker, waits until healthy, tests; keeps it running
python3 -m pytest tests -q --docker run    # the same, then stops it if all passed (on a failure it keeps running)
python3 -m pytest tests -q --docker down   # the same, then stops it whatever the result
COMPAT_DRY=1 python3 -m pytest tests/compat -q -s                  # no Docker: dynamoredis side only
```

**Not supported** — raises `NotSupportedError`, never silently wrong: pub/sub, blocking commands (`BLPOP` …),
Lua / functions, `MULTI` / `EXEC` / `WATCH` (`pipeline(transaction=True)`), streams, `CONFIG` / `CLIENT` /
replication / cluster. They need a server that holds connections and sessions; there is none here.

**Worth knowing:**
- Expiry has one-second precision: `PX` / `PEXPIRE` round up to the next second, `PTTL` is the TTL in seconds × 1000.
- `KEYS`, `SCAN`, `DBSIZE` and `FLUSHDB` scan the whole table: on a large table each call costs read
  capacity in proportion to the table's size.
- Two clients changing the same key at the same moment: the command that lost the race runs again
  automatically; if the key keeps changing it gives up after 3 retries with
  `ResponseError("ERR concurrent modification of key '…', retried 3 times")` — never a silent overwrite.

## Guarantees

| | How |
|---|---|
| **Atomicity** | one command = one DynamoDB operation with a condition expression; counters, pushes and set changes are server-side `ADD` / `list_append` / `DELETE`; `RENAME` is one transaction |
| **No lost writes** | a command that reads first (`LPOP`, `LTRIM`, `HDEL`, `APPEND`, …) writes only if the key is still what it read; otherwise it runs again (3 retries, then an error). A write never lands in a key that has just expired |
| **Durability** | the client gets the reply after DynamoDB has committed (multi-AZ) |
| **Isolation** | per key, as in Redis; no cross-key transactions (`MULTI` is refused, see TASKS.md task 1) |
| **Expiry** | `exp` is checked on every read, so an expired key is gone at once; DynamoDB TTL reclaims the item within hours |
| **No duplicate writes** | the driver re-sends only when the request provably never reached the server; timeouts raise `TimeoutError` with the result unknown |
| **Availability** | Lambda and DynamoDB are regional, multi-AZ services: no node to fail, no failover, no maintenance window |

## Who this is for

Caches, counters, rate limits, sessions, feature flags, small work queues polled by Lambdas — anything
that is idle most of the time. Latency is HTTPS + DynamoDB (tens of milliseconds, not microseconds), and
`KEYS` / `DBSIZE` scan the table. If you need pub/sub, blocking pops, Lua or transactions, that is the
moment to move to a real Redis — see [MIGRATION.md](MIGRATION.md).

## Repository layout

```
src/server/       the service: lambda_function.py (HTTP API + console), engine.py (redis → DynamoDB), index.html
client/           the pip package dynamoredis: dynamoredis/redis.py, pyproject.toml
terraform/        deployment, option A
cloudformation/   deployment, option B
tests/            regression suite (moto); tests/compat/ compatibility suite, REPORT.md, TASKS.md
```

## Releasing

CI runs the whole suite, Redis included, on every push. A release is a tag: bump the version in
`client/pyproject.toml` and `client/dynamoredis/__init__.py`, add the CHANGELOG entry, commit, then
`git tag vX.Y.Z && git push --tags` (tests → build → PyPI; the tag must equal the version).

---

Part of **[Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless)** — AWS services that bill
while idle, rebuilt on primitives that cost ≈ $0 when idle. MIT.
