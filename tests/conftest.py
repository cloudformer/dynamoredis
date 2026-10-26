"""Test setup: src/server and client/ on the import path, a mocked DynamoDB table (moto), the report hooks.

The report hooks must live in the ROOT conftest so they see the whole run (regression + compat).
"""
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "server"))
sys.path.insert(0, os.path.join(ROOT, "client"))

# the pip / import name, read from client/pyproject.toml so nothing else hard-codes it
with open(os.path.join(ROOT, "client", "pyproject.toml"), encoding="utf-8") as _f:
    PACKAGE = re.search(r'^name\s*=\s*"([^"]+)"', _f.read(), re.M).group(1)
os.environ["PACKAGE"] = PACKAGE

# lambda_function / boto3 read these at import time
os.environ.setdefault("USER_NAME", "admin")
os.environ.setdefault("PASSWORD", "pw")
os.environ.setdefault("TABLE", "dynamoredis-test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

from compat_report import (pytest_collection_modifyitems, pytest_configure,  # noqa: E402,F401
                           pytest_runtest_logreport, pytest_sessionfinish, pytest_terminal_summary)


@pytest.fixture
def table():
    """A fresh mocked DynamoDB table per test, wired into the server's engine."""
    import boto3
    from moto import mock_aws

    import lambda_function as lf
    from engine import Engine

    with mock_aws():
        db = boto3.resource("dynamodb")
        db.create_table(TableName=os.environ["TABLE"], KeySchema=[{"AttributeName": "k", "KeyType": "HASH"}],
                        AttributeDefinitions=[{"AttributeName": "k", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
        lf._engine = Engine(os.environ["TABLE"], client=db)
        yield lf._engine
        lf._engine = None
