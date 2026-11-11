PROGRAM(fastshard-jdserver)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    app.cpp
    bootstrap.cpp
    main.cpp
)

PEERDIR(
    cloud/fastshard/apps/jdserver/lib

    cloud/fastshard/libs/journal/impl
    cloud/fastshard/libs/journal/server

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics

    library/cpp/logger
)

END()
