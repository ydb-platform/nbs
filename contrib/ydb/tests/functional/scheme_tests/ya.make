PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    tablet_scheme_tests.py
)

SIZE(MEDIUM)

DEPENDS(
)

DATA(
    arcadia/contrib/ydb/tests/functional/scheme_tests/canondata
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/canonical
)

END()
