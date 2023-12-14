PROGRAM(blockstore-loadtest)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/tools/testing/loadtest/lib
)

END()
