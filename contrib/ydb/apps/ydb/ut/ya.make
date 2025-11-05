UNITTEST()

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
)

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_FEATURE_FLAGS="enable_topic_service_tx")

SRCS(
    workload-topic.cpp
    workload-transfer-topic-to-table.cpp
    run_ydb.cpp
    supported_codecs.cpp
    supported_codecs_fixture.cpp
    ydb-dump.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

END()
