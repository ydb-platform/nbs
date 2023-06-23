LIBRARY()

SRCS(
    protocol.cpp
    rdma_client.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()
