PY3TEST()

SUBSCRIBER(g:yql)

TEST_SRCS(test.py)

PEERDIR(
    contrib/ydb/library/benchmarks/report
)

END()
