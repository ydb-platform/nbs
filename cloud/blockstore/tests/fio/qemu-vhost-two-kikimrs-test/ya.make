PY3TEST()

IF (OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()

TAG(ya:manual)

DEPENDS(
    cloud/storage/core/tools/testing/fio/bin
)

PEERDIR(
    cloud/blockstore/tests/python/lib
    cloud/storage/core/tools/testing/fio/lib
)

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/two-local-kikimrs/two-local-kikimrs.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/endpoint-two-clusters/vhost-endpoint-two-clusters.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/qemu-two-clusters.inc)

END()
