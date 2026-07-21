LIBRARY()

SRCS(
    memory_controller.cpp
    memtable_collection.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/mon_alloc
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/tablet
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/utils/memory_profiling
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
