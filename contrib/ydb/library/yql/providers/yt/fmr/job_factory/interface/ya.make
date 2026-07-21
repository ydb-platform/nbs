LIBRARY()

SRCS(
    yql_yt_job_factory.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
