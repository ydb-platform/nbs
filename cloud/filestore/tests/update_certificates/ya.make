PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/server
    cloud/filestore/apps/vhost
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

    contrib/python/requests/py3

    library/python/testing/yatest_common
)

END()
