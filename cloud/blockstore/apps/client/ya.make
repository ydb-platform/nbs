PROGRAM(blockstore-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/terminate_handler
    cloud/blockstore/apps/client/lib
    cloud/storage/core/libs/iam/iface
)

END()

RECURSE(
    lib
)
