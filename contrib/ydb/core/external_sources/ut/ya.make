UNITTEST_FOR(contrib/ydb/core/external_sources)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    object_storage_ut.cpp
    external_data_source_ut.cpp
)

END()
