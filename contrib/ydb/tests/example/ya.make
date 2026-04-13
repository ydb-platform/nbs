PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_example.py  # TODO: change file name to yours
)

SIZE(MEDIUM)


PEERDIR(
    contrib/ydb/tests/library
)

END()
