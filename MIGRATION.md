# Moving to Redis / ElastiCache

dynamoredis is for the long tail: caches, counters, sessions, small queues that sit idle most of the time.
When you need pub/sub, blocking queues, Lua, transactions or sub-millisecond latency, a real Redis is one
import line away.

1. Create the Redis: ElastiCache Serverless (or Valkey), or any Redis 7.
2. Copy the data: the console shows every key; for more than a few hundred keys run a small script with
   `scan_iter()` on dynamoredis and `restore`-free writes (`set` / `hset` / `rpush` / `sadd` / `zadd`) on the
   new server. Values are plain Redis strings, nothing to convert.
3. Change the import: `from dynamoredis import redis` → `import redis`. `Redis(host=, username=, password=)`
   keeps the same shape; drop `pipeline(transaction=False)` back to the default if you like.
4. Delete the stack: `terraform -chdir=terraform destroy -var-file=my.json` (the table goes with it).

What you gain: pub/sub, `BLPOP`, Lua, `MULTI`/`EXEC`, latency in microseconds. What you lose: the ≈ $0 idle bill.
