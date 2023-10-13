PROGRAM(blockstore-client)

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
ELSE()
    ALLOCATOR(TCMALLOC_TC)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    cloud/blockstore/apps/client/lib
)

END()

RECURSE(
    lib
)
