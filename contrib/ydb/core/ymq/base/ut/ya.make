UNITTEST()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/ymq/base
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    action_ut.cpp
    counters_ut.cpp
    dlq_helpers_ut.cpp
    helpers_ut.cpp
    secure_protobuf_printer_ut.cpp
    queue_attributes_ut.cpp
)

END()
