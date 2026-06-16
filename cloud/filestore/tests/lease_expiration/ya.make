PY3TEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ENDIF()

TEST_SRCS(test.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    cloud/storage/core/tools/testing/qemu/lib

    contrib/ydb/core/protos
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

    contrib/ydb/apps/ydbd
)

DATA(arcadia/cloud/storage/core/tools/testing/qemu/keys)

END()
