LIBRARY()

SRCS(
    client_test.cpp
    memory_test_storage.cpp
    rdma_test_environment.cpp
    server_test_async.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future
)

END()
