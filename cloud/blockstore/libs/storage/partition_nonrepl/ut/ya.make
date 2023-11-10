UNITTEST_FOR(cloud/blockstore/libs/storage/partition_nonrepl)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    part_mirror_resync_ut.cpp
    part_mirror_state_ut.cpp
    part_mirror_ut.cpp
    part_nonrepl_common_ut.cpp
    part_nonrepl_migration_state_ut.cpp
    part_nonrepl_migration_ut.cpp
    part_nonrepl_rdma_ut.cpp
    part_nonrepl_ut.cpp
    resync_range_ut.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/testlib
)

END()
