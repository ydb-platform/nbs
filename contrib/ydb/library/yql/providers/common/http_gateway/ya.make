LIBRARY()

SRCS(
    yql_aws_signature.cpp
    yql_http_default_retry_policy.cpp
    yql_http_gateway.cpp
)

PEERDIR(
    contrib/libs/curl
    library/cpp/monlib/dynamic_counters
    library/cpp/retry
    contrib/ydb/library/actors/http
    contrib/ydb/library/actors/prof
    contrib/ydb/library/actors/protos
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mock
)

RECURSE_FOR_TESTS(
    ut
)
