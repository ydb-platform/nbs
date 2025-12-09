PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
