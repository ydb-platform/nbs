PROGRAM(blockstore-nbd)

SRCS(
    app.cpp
    bootstrap.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/blockstore/config
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/encryption/model
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/version
    library/cpp/lwtrace/mon
    library/cpp/getopt
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/util
    library/cpp/deprecated/atomic
)

IF(NETLINK)
    CFLAGS(
        -DNETLINK
    )
ENDIF()

END()
