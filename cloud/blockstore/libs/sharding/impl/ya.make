LIBRARY()

SRCS(
    describe_volume.cpp
    endpoints_setup.cpp
    shard_host.cpp
    remote_storage.cpp
    shard.cpp
    sharding.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/sharding/iface
)

END()

RECURSE_FOR_TESTS(ut)
