Y_BENCHMARK()

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics

    library/cpp/getopt
    library/cpp/logger
    library/cpp/resource
    library/cpp/sighandler
)

RESOURCE(
    res/client_stats.json           client_stats
    res/client_volume_stats.json    client_volume_stats
)

END()
