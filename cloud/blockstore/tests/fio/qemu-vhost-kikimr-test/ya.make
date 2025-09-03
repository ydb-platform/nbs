PY3TEST()

IF (OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()

TAG(ya:manual)

ENV(NBS_INSTANCE_COUNT=1)

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

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/local-kikimr/local-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/endpoint/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/qemu.inc)

END()
