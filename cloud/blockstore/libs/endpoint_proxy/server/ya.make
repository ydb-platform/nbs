LIBRARY()

SRCS(
    bootstrap.cpp
    options.cpp
    proxy_storage.cpp
    server.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/common
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/uds
    cloud/storage/core/protos

    library/cpp/getopt/small
    library/cpp/logger

    contrib/libs/grpc
    contrib/ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(ut)
