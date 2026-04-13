PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

TEST_SRCS(
    test_workload.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32)

DEPENDS(
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
)


END()
