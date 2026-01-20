UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

ENV(YDB_FEATURE_FLAGS="enable_topic_transfer")
ENV(YDB_GRPC_SERVICES="replication")

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/draft
)

SRCS(
    replication.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:24 cpu:4)
ELSE()
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

END()
