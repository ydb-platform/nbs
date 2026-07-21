LIBRARY()

SRCS(
    yql_yt_coordinator_proto_helpers.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/proto
    contrib/ydb/library/yql/providers/yt/fmr/request_options/proto_helpers
)

YQL_LAST_ABI_VERSION()

END()
