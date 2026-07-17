PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tools/testing/loadtest/bin
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/filestore/tools/testing/loadtest/protos
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/diagnostic_test/nfs-storage.txt
)
SET(
    NFS_DIAG_CONFIG_PATCH
    cloud/filestore/tests/diagnostic_test/nfs-diag.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
