PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tools/analytics/profile_tool
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/filestore/tools/testing/profile_log
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/client/nfs-storage.txt
)

SET(NFS_TRACE_SAMPLING_RATE 1)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
