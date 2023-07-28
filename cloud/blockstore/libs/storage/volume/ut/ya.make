UNITTEST_FOR(cloud/blockstore/libs/storage/volume)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)
ENDIF()

SRCS(
    volume_database_ut.cpp
    volume_state_ut.cpp
    volume_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/testlib
    cloud/blockstore/libs/storage/volume/testlib
)

END()
