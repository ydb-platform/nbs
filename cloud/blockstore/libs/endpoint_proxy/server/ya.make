LIBRARY()

SRCS(
    bootstrap.cpp
    options.cpp
    server.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/common

    library/cpp/getopt/small

    contrib/ydb/library/actors/util
)

END()
