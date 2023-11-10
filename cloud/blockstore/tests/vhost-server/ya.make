PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/vhost-server
)

DATA(
)

PEERDIR(
)

END()
