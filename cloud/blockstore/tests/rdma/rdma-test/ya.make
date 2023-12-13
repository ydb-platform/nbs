PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/rdma/rdma.inc)

IF (OPENSOURCE) # TODO(NBS-4409): enable test for opensource
    TAG(
        ya:not_autocheck
        ya:manual
    )
ENDIF()

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
