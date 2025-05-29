LIBRARY()

SRCS(
    config.cpp
    describe_volume.cpp
    remote_storage_provider.cpp
    remote_storage.cpp
    shard_client.cpp
    shard_endpoints_manager.cpp
    sharding_common.cpp
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
