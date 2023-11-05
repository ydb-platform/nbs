UNITTEST()

SRCS(
    queue_id_ut.cpp
    params_ut.cpp
)

PEERDIR(
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/http
    contrib/ydb/library/http_proxy/error
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
