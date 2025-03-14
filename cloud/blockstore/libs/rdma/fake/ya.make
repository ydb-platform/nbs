LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb
    cloud/storage/core/libs/kikimr
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
