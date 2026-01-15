LIBRARY()

SRCS(
    bootstrap.cpp
    cell_host_impl.cpp
    cell_host.cpp
    cell_impl.cpp
    cell_manager_impl.cpp
    cell_manager.cpp
    cell.cpp
    describe_volume.cpp
    endpoint_bootstrap_impl.cpp
    endpoint_bootstrap.cpp
    remote_storage.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
)

END()

RECURSE_FOR_TESTS(ut)
