PROGRAM(fastshard-client)

ALLOCATOR(TCMALLOC_256K)

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
