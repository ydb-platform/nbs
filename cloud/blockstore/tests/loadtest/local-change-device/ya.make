PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/disk_agent
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-change-device
)

END()
