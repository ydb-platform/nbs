PROGRAM(fastshard-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/fastshard_client/lib
)

END()

RECURSE(
    lib
)
