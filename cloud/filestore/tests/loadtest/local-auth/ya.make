PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

SET(TEST_TOKEN "test_iam_token")

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
    cloud/storage/core/tools/testing/access_service/mock
)

DATA(
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
    arcadia/cloud/filestore/tests/loadtest/local-auth
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/access-service.inc)

SET_APPEND(RECIPE_ARGS
    --secure
)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
