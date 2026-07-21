LIBRARY()

SRCS(
    yql_yt_request_options.cpp
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/threading/future
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/tvm/interface
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_request_options.h)

END()

RECURSE(
    proto_helpers
)
