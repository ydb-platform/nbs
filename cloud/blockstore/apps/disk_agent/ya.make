PROGRAM(diskagentd)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/disk_agent
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/spdk/iface

    cloud/storage/core/libs/daemon

    ydb/core/security
    ydb/library/keys

    library/cpp/getopt
)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG" AND BUILD_TYPE != "RELWITHDEBINFO")
    SPLIT_DWARF()
ELSE()
    PEERDIR(
        library/cpp/terminate_handler
    )
ENDIF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

YQL_LAST_ABI_VERSION()

END()
