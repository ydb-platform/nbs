UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/public/lib/ut_helpers
    contrib/ydb/public/sdk/cpp/client/ydb_discovery
    contrib/ydb/public/sdk/cpp/client/ydb_query
    contrib/ydb/public/sdk/cpp/client/draft
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
TIMEOUT(30)

REQUIREMENTS(ram:16)

END()
