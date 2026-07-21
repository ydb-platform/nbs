UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines/storage/indexes/bits_storage)

SRCS(
    bits_storage_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
