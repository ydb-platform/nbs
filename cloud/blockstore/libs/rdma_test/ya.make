LIBRARY()

SRCS(
    client_test.cpp
    server_test.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future
)

END()
