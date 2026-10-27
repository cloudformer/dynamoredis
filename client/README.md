# dynamoredis

**Python client for [dynamoredis](https://github.com/cloudformer/dynamoredis) — a truly serverless Redis:
one DynamoDB table, a Lambda Function URL as the endpoint, ≈ $0 when idle.** The redis-py API, standard
library only. Part of [Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless).

![dynamoredis architecture](https://raw.githubusercontent.com/cloudformer/dynamoredis/main/docs/architecture.svg)

```bash
pip install dynamoredis
```

```python
from dynamoredis import redis        # later: `import redis` against a real Redis, nothing else changes

r = redis.Redis(host="xxx.lambda-url.us-east-2.on.aws", username="admin", password="...")
r.set("greeting", "hello", ex=60)
r.get("greeting")                    # b"hello"
r.incr("hits")                       # atomic (DynamoDB ADD)
r.hset("user:1", mapping={"name": "alice", "age": 30})
r.zadd("board", {"alice": 10, "bob": 20}); r.zrange("board", 0, -1, withscores=True)
with r.pipeline(transaction=False) as p:
    p.set("a", 1); p.get("a"); p.execute()     # one request
```

Strings, keys/TTL, hashes, lists, sets, sorted sets, pipelines. Not supported (raises `NotSupportedError`):
pub/sub, blocking commands, Lua, `MULTI`/`EXEC`/`WATCH`, streams — those need a server that holds
connections; that is the moment to move to a real Redis.

The endpoint comes from deploying the [main repository](https://github.com/cloudformer/dynamoredis) —
same as any client needs its server. Compatibility with redis-py against Redis 7:
[REPORT.md](https://github.com/cloudformer/dynamoredis/blob/main/tests/compat/REPORT.md).
