PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/storage/core/tools/testing/access_service/mock
    cloud/storage/core/tools/testing/access_service_nebius/mock
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/loadtest/local-auth
)

TIMEOUT(3600)

END()
