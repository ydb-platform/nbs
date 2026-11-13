PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/filestore/apps/server
)

DATA(
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
    arcadia/cloud/filestore/tests/certs/server_new.crt
    arcadia/cloud/filestore/tests/certs/server_new.key
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/sdk/python/client
    cloud/filestore/tests/python/lib

    library/python/testing/yatest_common
)

END()
