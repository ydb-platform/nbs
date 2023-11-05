UNITTEST()

PEERDIR(
    contrib/ydb/core/ymq/http
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    xml_builder_ut.cpp
)

END()
