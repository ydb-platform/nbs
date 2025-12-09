UNITTEST()

SRCS(
    queue_id_ut.cpp
    params_ut.cpp
)

PEERDIR(
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/http
    contrib/ydb/library/http_proxy/error
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
