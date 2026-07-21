UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines/storage/indexes/helper)

SRCS(
    case_helper_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
