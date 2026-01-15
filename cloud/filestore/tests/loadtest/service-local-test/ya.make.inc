INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/filestore/tests/loadtest/service-local-test
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-local.inc)
