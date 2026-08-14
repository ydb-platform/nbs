PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/vhost
    cloud/storage/core/tools/testing/qemu/bin
    cloud/storage/core/tools/testing/qemu/image-noble
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib
    cloud/storage/core/protos
    cloud/storage/core/tools/testing/qemu/lib

    library/python/testing/yatest_common
)

DATA(arcadia/cloud/storage/core/tools/testing/qemu/keys)

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tools/testing/qemu/bin/qemu-params.inc)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
