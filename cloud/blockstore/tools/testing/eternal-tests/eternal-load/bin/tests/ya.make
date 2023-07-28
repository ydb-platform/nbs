PY2TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/testing/eternal-tests/eternal-load/bin
)

PEERDIR(
    contrib/deprecated/python/futures
)

END()
