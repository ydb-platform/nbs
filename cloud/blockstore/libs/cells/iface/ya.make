LIBRARY()

SRCS(
    cell.cpp
    cells.cpp
    config.cpp
    host_endpoint.cpp
    host.cpp
    remote_storage.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service
)

END()
