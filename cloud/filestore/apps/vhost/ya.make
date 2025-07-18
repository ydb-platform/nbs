PROGRAM(filestore-vhost)

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
ELSE()
    ALLOCATOR(TCMALLOC_TC)
ENDIF()

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
    cloud/filestore/libs/daemon/vhost
    cloud/filestore/libs/vfs_fuse/vhost

    cloud/storage/core/libs/daemon

    contrib/ydb/core/security
    contrib/ydb/library/keys
)

YQL_LAST_ABI_VERSION()

END()
