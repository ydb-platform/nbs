LIBRARY()

SRCS(
    config.cpp
    describe_volume.cpp
    endpoints_setup.cpp
    host_endpoint.cpp
    remote_storage.cpp
    sharding_common.cpp
    host_endpoints_manager.cpp
    shard_manager.cpp
    sharding_manager.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core

    cloud/storage/core/libs/auth

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
