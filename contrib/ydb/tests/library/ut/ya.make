PY3TEST()

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/library/yql/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
