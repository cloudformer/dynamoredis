"""dynamoredis — a truly serverless Redis: Lambda Function URL endpoint, DynamoDB storage, ≈ $0 when idle.

    from dynamoredis import redis          # later: `import redis` against a real Redis, nothing else changes
    r = redis.Redis(host="xxx.lambda-url.us-east-2.on.aws", username="admin", password="...")
"""
from . import redis  # noqa: F401

__version__ = "0.1.2"
