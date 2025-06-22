UNITTEST_FOR(cloud/blockstore/libs/sharding/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    describe_volume_ut.cpp
    shard_host_ut.cpp
    shard_ut.cpp
    sharding_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
    cloud/blockstore/libs/sharding/iface
    cloud/blockstore/libs/sharding/impl
)

END()
