LIBRARY()

SRCS(
    rdma_server_handler.cpp
    rdma_tablet_forwarder.cpp
    rdma_tablet_registry.cpp
    rdma_tablet_target.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/rdma
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/rdma/iface

    contrib/ydb/library/actors/core

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
