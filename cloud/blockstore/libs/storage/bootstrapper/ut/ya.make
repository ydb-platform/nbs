UNITTEST_FOR(cloud/blockstore/libs/storage/bootstrapper)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    bootstrapper_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

END()
