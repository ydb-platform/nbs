PROGRAM(fastshard-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/fastshard/apps/client/lib
)

END()

RECURSE(
    lib
)
