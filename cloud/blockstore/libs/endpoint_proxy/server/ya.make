LIBRARY()

SRCS(
    bootstrap.cpp
    options.cpp
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

    library/cpp/getopt/small
    library/cpp/logger

    contrib/libs/grpc
    contrib/ydb/library/actors/util
)

IF(NETLINK)
    CFLAGS(
        -DNETLINK
    )
ENDIF()

END()
