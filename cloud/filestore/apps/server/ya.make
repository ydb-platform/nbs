PROGRAM(filestore-server)

ALLOCATOR(TCMALLOC_TC)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/binaries_dependency.inc)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
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

    util/terminate_handler
)

YQL_LAST_ABI_VERSION()

END()
