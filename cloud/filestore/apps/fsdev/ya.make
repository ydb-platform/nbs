PROGRAM(filestore-fsdev)

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

ADDINCL(
    cloud/filestore/libs/spdk/lib/include
)

PEERDIR(
    cloud/filestore/libs/spdk/iface
    cloud/filestore/libs/spdk/impl

    cloud/storage/core/libs/common
)


YQL_LAST_ABI_VERSION()

END()
