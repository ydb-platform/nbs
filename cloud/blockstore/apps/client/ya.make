PROGRAM(blockstore-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    cloud/blockstore/client/lib
)

END()
