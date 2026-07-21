PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

ENV(YDB_DSTOOL_BINARY="contrib/ydb/apps/dstool/ydb-dstool")

PY_SRCS (
    common.py
    helpers.py
    vhost_user_blk_client.py
)
TEST_SRCS(
    test_nbs.py
    test_nbs_load_actor.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

REQUIREMENTS(ram:16)

DEPENDS(
    contrib/ydb/apps/dstool
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
)

END()
