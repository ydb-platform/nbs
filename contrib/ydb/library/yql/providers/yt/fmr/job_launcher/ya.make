LIBRARY()

SRCS(
    yql_yt_job_launcher.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/process
)

YQL_LAST_ABI_VERSION()

END()
