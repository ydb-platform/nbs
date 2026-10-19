PROGRAM(fastshard-jdserver)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/fastshard/apps/jdserver/lib
)

END()

RECURSE(
    lib
)
