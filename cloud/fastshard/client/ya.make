PROGRAM(fastshard-client)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/fastshard/client/lib
)

END()

RECURSE(
    lib
)
