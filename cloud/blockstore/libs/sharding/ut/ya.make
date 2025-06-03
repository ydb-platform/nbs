UNITTEST_FOR(cloud/blockstore/libs/sharding)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    describe_volume_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
    cloud/blockstore/libs/sharding
)

END()
