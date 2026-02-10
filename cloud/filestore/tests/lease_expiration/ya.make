PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    cloud/storage/core/tools/testing/qemu/lib

    contrib/ydb/tests/library
    contrib/python/requests/py3

    library/python/fs
    library/python/retry
    library/python/testing/yatest_common
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/server
    cloud/filestore/apps/vhost

    cloud/storage/core/tools/testing/qemu/bin
    cloud/storage/core/tools/testing/qemu/image
    cloud/storage/core/tools/testing/ydb/bin
)

DATA(arcadia/cloud/storage/core/tools/testing/qemu/keys)

END()
