PY3_PROGRAM(kafka_streams_test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/kafka_serverless/workload
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

END()

RECURSE_FOR_TESTS(
    tests
)
