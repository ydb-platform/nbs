PROGRAM(nbsd-lightweight)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
ENDIF()

PEERDIR(
    cloud/blockstore/libs/daemon/local
    cloud/blockstore/libs/service

    cloud/storage/core/libs/daemon
)

CHECK_DEPENDENT_DIRS(ALLOW_ONLY PEERDIRS
    build/platform
    certs
    cloud/blockstore
    cloud/contrib
    cloud/storage
    contrib/libs
    contrib/restricted
    library/cpp
    contrib/ydb/library/actors
    contrib/ydb/library/services
    logbroker
    tools/enum_parser
    util
)

END()
