PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

# This test relies on wall clock timeouts for region invalidation, which makes
# it unreliable with sanitizers
IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SPLIT_FACTOR(1)

PEERDIR(
    cloud/filestore/public/sdk/python/client
)

TEST_SRCS(
    test.py
)

SET(
    NFS_SERVER_CONFIG_PATCH
    cloud/filestore/tests/service/service-kikimr-shared-memory-region-invalidation-test/nfs-storage-server-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
