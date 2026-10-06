PROGRAM(filestore-vhost)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/binaries_dependency.inc)

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
ELSE()
    ALLOCATOR(TCMALLOC_256K)
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

# AOCL LibMem uses Zen 4 instructions and requires an AVX-512 capable CPU.
IF (OS_LINUX AND ARCH_X86_64)
    PEERDIR(
        contrib/libs/aocl-libmem
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
