PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/filestore/tests/loadtest/service-kikimr-newfeatures-test
)

PEERDIR(
    cloud/filestore/tests/python/lib

    contrib/python/requests/py3
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-newfeatures-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
