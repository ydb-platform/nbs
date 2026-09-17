LIBRARY()

SRCS(
    cell_manager.cpp
    config.cpp
    inbound_activity.cpp
    host_endpoint.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service

    cloud/storage/core/libs/rdma/iface
)

END()
