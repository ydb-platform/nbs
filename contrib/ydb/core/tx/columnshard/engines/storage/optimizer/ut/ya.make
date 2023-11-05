UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/changes
    contrib/ydb/core/tx/columnshard/engines
    contrib/ydb/core/tx/columnshard
    contrib/ydb/library/yql/public/udf
    contrib/ydb/core/formats/arrow/compression
    contrib/ydb/core/grpc_services
    contrib/ydb/core/scheme
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/mkql_proto
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/mkql_proto
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/core/persqueue
    contrib/ydb/core/tx/time_cast
    contrib/ydb/library/yql/sql/pg
)

SRCS(
    ut_optimizer.cpp
)

END()
