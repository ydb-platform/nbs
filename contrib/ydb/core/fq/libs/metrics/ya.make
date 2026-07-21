LIBRARY()

SRCS(
    sanitize_label.cpp
    status_code_counters.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/dq/actors/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
