PROGRAM(nbsd)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/ydb
    cloud/blockstore/libs/kms/iface
    cloud/blockstore/libs/kms/impl
    cloud/blockstore/libs/logbroker/iface
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/root_kms/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/service_su
    cloud/blockstore/libs/spdk/iface

    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/iam/iface

    contrib/ydb/core/security
    contrib/ydb/library/keys

    library/cpp/getopt
)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG" AND BUILD_TYPE != "RELWITHDEBINFO")
    SPLIT_DWARF()
ENDIF()

YQL_LAST_ABI_VERSION()

END()
