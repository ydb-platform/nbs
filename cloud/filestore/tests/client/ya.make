PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
