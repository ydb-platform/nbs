PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/library/yql/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
