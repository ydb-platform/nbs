PY3_LIBRARY()

PY_SRCS(
    workload_log.py
)

BUNDLE(
    contrib/ydb/apps/ydb NAME ydb_cli
)
RESOURCE(ydb_cli ydb_cli)
PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/public/sdk/python
    library/python/testing/yatest_common
    library/python/resource
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

END()
