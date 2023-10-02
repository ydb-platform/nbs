Y_BENCHMARK(blockstore-client-bench)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption
)

END()
