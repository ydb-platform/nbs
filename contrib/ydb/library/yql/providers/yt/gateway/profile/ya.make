LIBRARY()

SRCS(
    yql_yt_profiling.cpp
)

PEERDIR(
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
