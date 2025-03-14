Y_BENCHMARK(blockstore-nbd-bench)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

TAG(ya:not_autocheck)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    bootstrap.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service
)

END()
