PY3_LIBRARY()

PY_SRCS(
    custom_hooks.py
    fq_client.py
    kikimr_metrics.py
    kikimr_runner.py
    kikimr_utils.py
    mvp_mock.py
)

PEERDIR(
    contrib/python/requests
    library/python/retry
    library/python/testing/yatest_common
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/tests/library
)

END()
