PROGRAM(nbsd)

# Shared protocols and interfaces are allowed; tablet implementation and
# its private services must never enter the lightweight dependency graph.
CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    GLOB contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/columnshard/column_fetching
    contrib/ydb/core/tx/columnshard/data_accessor/cache_policy
    contrib/ydb/core/tx/columnshard/engines/reader
    contrib/ydb/core/tx/conveyor_composite/service
    contrib/ydb/core/tx/priorities/service
)

ALLOCATOR(TCMALLOC_256K)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/binaries_dependency.inc)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/ydb
    cloud/blockstore/libs/kms/iface
    cloud/blockstore/libs/kms/impl
    cloud/blockstore/libs/logbroker/iface
    cloud/blockstore/libs/notify/impl
    cloud/blockstore/libs/rdma
    cloud/blockstore/libs/root_kms/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/spdk/iface

    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/iam/iface
    cloud/storage/core/libs/opentelemetry/impl
    cloud/storage/core/libs/rdma/impl

    contrib/ydb/core/security
    contrib/ydb/library/keys

    library/cpp/getopt
)

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG" AND BUILD_TYPE != "RELWITHDEBINFO")
    SPLIT_DWARF()
ENDIF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

YQL_LAST_ABI_VERSION()

END()
