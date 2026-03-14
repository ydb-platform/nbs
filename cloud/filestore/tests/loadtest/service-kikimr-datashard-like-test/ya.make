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
    cloud/filestore/tests/loadtest/service-kikimr-datashard-like-test/nfs-storage-config-patch.txt
)

PEERDIR(
    cloud/filestore/tests/python/lib

    contrib/python/requests/py3
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/service/service-kikimr-parentless-handleless-test/nfs-storage-config-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
