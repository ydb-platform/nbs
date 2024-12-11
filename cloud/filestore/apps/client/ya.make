PROGRAM(filestore-client)

ALLOCATOR(TCMALLOC_TC)

SPLIT_DWARF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/apps/client/lib
    cloud/storage/core/libs/iam/iface
    library/cpp/getopt
)

END()

RECURSE(
    lib
)
