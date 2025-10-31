PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    library/python/testing/yatest_common

    ydb/tests/library

    contrib/python/requests/py3
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/server
    cloud/filestore/apps/vhost
    ydb/apps/ydbd
)

END()
