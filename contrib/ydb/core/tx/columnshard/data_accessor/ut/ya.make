UNITTEST_FOR(contrib/ydb/core/tx/columnshard/data_accessor)

SRCS(
    ut_manager.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/ydb/core/tx/general_cache/usage
    contrib/ydb/library/yverify_stream
    contrib/ydb/core/tx/columnshard
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/actors/core
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()