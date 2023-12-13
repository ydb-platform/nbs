PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/rdma/rdma.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/testing/rdma-test
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
