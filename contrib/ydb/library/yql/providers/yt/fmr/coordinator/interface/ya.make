LIBRARY()

SRCS(
    yql_yt_coordinator.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
