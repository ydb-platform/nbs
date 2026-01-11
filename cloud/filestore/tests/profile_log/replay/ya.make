RECURSE_FOR_TESTS(data)

PY3TEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ENDIF()

DEPENDS(
    cloud/filestore/tools/testing/loadtest/bin
    cloud/filestore/tests/profile_log/replay/data
)

PEERDIR(
    cloud/filestore/tools/testing/loadtest/protos
)

TEST_SRCS(
    test.py
    test_grpc.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/mount.inc)

END()
