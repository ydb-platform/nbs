PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
)

DATA(
    arcadia/cloud/filestore/tests/loadtest/service-kikimr-localdb-compaction-test
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib
)

IF (SANITIZER_TYPE == "thread")
    SET(NFS_RESTART_INTERVAL 20)
ELSE()
    SET(NFS_RESTART_INTERVAL 5)
ENDIF()

SET(NFS_FORCE_VERBOSE 1)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

END()
