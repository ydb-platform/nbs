PY3TEST()

OWNER(g:cloud-nbs)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

PEERDIR(
    cloud/blockstore/pylibs/common
)

DEPENDS(
    cloud/blockstore/apps/disk_agent
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-change-device
)

END()
