PROGRAM(fastshard-jdserver)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/fastshard/jdserver/lib
)

END()

RECURSE(
    lib
)
