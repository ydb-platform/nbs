PY3TEST()

TIMEOUT(21600)

TAG(ya:manual)

SIZE(LARGE)

TEST_SRCS (
    test_large_import.py
)

PEERDIR (
    contrib/python/boto3
    contrib/ydb/public/sdk/python
    contrib/ydb/tests/olap/lib
)

END()
