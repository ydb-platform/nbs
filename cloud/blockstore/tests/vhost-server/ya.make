PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/vhost-server
    cloud/blockstore/tests/vhost-server/run_and_die
)

DATA(
)

END()
