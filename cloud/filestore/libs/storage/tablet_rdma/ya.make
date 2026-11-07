LIBRARY()

SRCS(
    rdma_server_handler.cpp
    rdma_tablet_forwarder.cpp
)

PEERDIR(
    cloud/filestore/libs/rdma
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/kikimr

    contrib/ydb/core/tablet
    contrib/ydb/library/actors/core

    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
