PY3TEST()

SPLIT_FACTOR(1)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/filestore/tests/loadtest/service-kikimr-datashard-like-test
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/loadtest/service-kikimr-datashard-like-test/nfs-storage-config-patch.txt
)

SET(
    NFS_SERVER_CONFIG_PATCH
    cloud/filestore/tests/loadtest/service-kikimr-datashard-like-test/nfs-storage-server-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
