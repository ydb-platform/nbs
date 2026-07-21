LIBRARY()

SRCS(
    yql_yt_job.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
