PROGRAM()

SRCS(
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb
    cloud/storage/core/libs/common

    library/cpp/getopt
    library/cpp/protobuf/util
)

END()
