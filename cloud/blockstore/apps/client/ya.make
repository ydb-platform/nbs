PROGRAM(blockstore-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    cloud/blockstore/apps/client/lib
    cloud/storage/core/libs/iam/iface
    cloud/storage/core/libs/terminate_handler
)

END()

RECURSE(
    lib
)
