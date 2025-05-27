LIBRARY()

SRCS(
    endpoint_events.cpp
    endpoint_listener.cpp
    endpoint_manager.cpp
    service_endpoint.cpp
    session_manager.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/sharding
    cloud/blockstore/libs/validation
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/endpoints/fs
    cloud/storage/core/libs/endpoints/keyring

    contrib/ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
