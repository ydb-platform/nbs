UNITTEST_FOR(cloud/blockstore/libs/storage/partition)

IF (SANITIZER_TYPE OR WITH_VALGRIND OR OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    part_database_ut.cpp
    part_state_ut.cpp
    part_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
