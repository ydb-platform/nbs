LIBRARY()

SRCS(
    yql_yt_request_proto_helpers.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    contrib/ydb/library/yql/providers/yt/fmr/proto
)

YQL_LAST_ABI_VERSION()

END()
