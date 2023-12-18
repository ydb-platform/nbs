UNITTEST_FOR(cloud/blockstore/libs/storage/volume)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    volume_checkpoint_ut.cpp
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
