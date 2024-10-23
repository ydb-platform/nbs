PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/filestore/tests/loadtest/service-kikimr-emergency-test
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/loadtest/service-kikimr-emergency-test/nfs-storage.txt
)

SET(NFS_BS_CACHE_FILE_PATH "bs_cache.txt")

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
