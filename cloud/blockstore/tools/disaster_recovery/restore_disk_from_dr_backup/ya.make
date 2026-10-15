PROGRAM()

SRCS(
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/protos_ydb

    library/cpp/getopt
)

END()
