PY3TEST()

PEERDIR(
    contrib/ydb/public/tools/lib/cmds
    contrib/ydb/library/yql/providers/common/proto
)

TEST_SRCS(
    test.py
)

END()
