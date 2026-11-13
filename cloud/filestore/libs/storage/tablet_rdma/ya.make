LIBRARY()

SRCS(
    rdma_server_handler.cpp
    rdma_tablet_forwarder.cpp
    rdma_tablet_registry.cpp
)

PEERDIR(
    cloud/filestore/libs/rdma
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core

    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
