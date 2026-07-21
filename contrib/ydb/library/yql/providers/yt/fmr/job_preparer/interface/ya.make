LIBRARY()

SRCS(
    yql_yt_job_preparer_interface.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/utils
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
