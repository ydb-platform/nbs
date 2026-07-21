LIBRARY()

SRCS(
    yql_yt_coordinator_client.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface/proto_helpers
    contrib/ydb/library/yql/providers/yt/fmr/proto
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
