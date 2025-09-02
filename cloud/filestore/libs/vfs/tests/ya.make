UNITTEST_FOR(cloud/filestore/libs/vfs)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ENDIF()

SRCS(
    fsync_queue_ut_stress.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
)

END()
