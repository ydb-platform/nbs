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

    contrib/ydb/core/security
    contrib/ydb/library/keys

    library/cpp/getopt
)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
ENDIF()

YQL_LAST_ABI_VERSION()

END()
