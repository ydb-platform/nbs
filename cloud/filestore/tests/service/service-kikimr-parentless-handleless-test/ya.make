PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

PEERDIR(
    cloud/filestore/public/sdk/python/client
)

TEST_SRCS(
    shared_memory_test.py
    test.py
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/service/service-kikimr-parentless-handleless-test/nfs-storage-config-patch.txt
)

SET(
    NFS_SERVER_CONFIG_PATCH
    cloud/filestore/tests/service/service-kikimr-parentless-handleless-test/nfs-storage-server-patch.txt
)


INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
