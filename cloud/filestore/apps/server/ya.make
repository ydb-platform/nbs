PROGRAM(filestore-server)

ALLOCATOR(TCMALLOC_TC)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/libs/daemon/server
    cloud/storage/core/libs/daemon

    ydb/core/security
)

YQL_LAST_ABI_VERSION()

END()
