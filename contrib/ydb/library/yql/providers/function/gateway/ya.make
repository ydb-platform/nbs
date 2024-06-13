LIBRARY()

SRCS(
    dq_function_gateway.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/function/common
    library/cpp/threading/future
)

YQL_LAST_ABI_VERSION()

END()