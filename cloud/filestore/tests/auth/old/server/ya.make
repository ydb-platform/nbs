PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/storage/core/tools/testing/access_service/mock
)

DATA(
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
)

PEERDIR(
    cloud/filestore/tests/auth/lib
    cloud/filestore/tests/python/lib
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/access-service.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
