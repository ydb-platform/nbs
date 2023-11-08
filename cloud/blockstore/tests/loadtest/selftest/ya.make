PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tests/loadtest/selftest/data
    cloud/blockstore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/selftest
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()

RECURSE(
    data
)
