PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
TEST_SRCS(
    tablet_scheme_tests.py
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/contrib/ydb/tests/functional/scheme_tests/canondata
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/canonical
)

END()
