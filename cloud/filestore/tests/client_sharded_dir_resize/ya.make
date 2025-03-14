PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
)

PEERDIR(
    cloud/filestore/tests/python/lib
)


SET(NFS_RESTART_INTERVAL 20)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/client_sharded_dir_resize/nfs-storage.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
