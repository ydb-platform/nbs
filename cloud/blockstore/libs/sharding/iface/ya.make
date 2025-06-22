LIBRARY()

SRCS(
    config.cpp
    endpoints_setup.cpp
    host_endpoint.cpp
    remote_storage.cpp
    shard_host.cpp
    shard.cpp
    sharding_arguments.cpp
    sharding.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service
)

END()
