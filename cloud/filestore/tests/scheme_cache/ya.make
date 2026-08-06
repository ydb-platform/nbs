PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/scheme_cache/nfs-storage.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
