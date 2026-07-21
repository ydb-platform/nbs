UNITTEST()

PEERDIR(
    contrib/ydb/core/http_proxy/sqs_xml
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    params_ut.cpp
    xml_builder_ut.cpp
)

END()
