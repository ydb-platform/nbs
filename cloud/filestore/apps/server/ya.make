PROGRAM(filestore-server)

ALLOCATOR(TCMALLOC_TC)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
ELSE()
    PEERDIR(
        library/cpp/terminate_handler
    )
ENDIF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/libs/daemon/server
    cloud/storage/core/libs/daemon

    contrib/ydb/core/security
    contrib/ydb/library/keys
)

YQL_LAST_ABI_VERSION()

END()
