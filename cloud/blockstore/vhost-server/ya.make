PROGRAM(blockstore-vhost-server)

SRCS(
    main.cpp

    histogram.cpp
    options.cpp
    request.cpp
    server.cpp
    stats.cpp
)

SPLIT_DWARF()

ADDINCL(
)

PEERDIR(
    cloud/contrib/vhost
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    ydb/library/actors/util
    library/cpp/getopt
    library/cpp/getopt/small
    library/cpp/logger

    contrib/libs/libaio
)

END()

RECURSE_FOR_TESTS(ut)
