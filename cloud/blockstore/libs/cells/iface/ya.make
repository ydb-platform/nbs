LIBRARY()

SRCS(
    cell_host.cpp
    cell_manager.cpp
    cell.cpp
    config.cpp
    host_endpoint.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service
)

END()
