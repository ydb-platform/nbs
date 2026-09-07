LIBRARY()

SRCS(
    bootstrap.cpp
    cell_manager_impl.cpp
    cell_manager.cpp
    connection.cpp
    describe_volume.cpp
    endpoint_bootstrap_impl.cpp
    endpoint_bootstrap.cpp
    endpoint_router.cpp
    host_pool.cpp
    remote_storage.cpp
    transport_switcher.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service

    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/rdma/impl

    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(ut)
