LIBRARY()

SRCS(
    yql_yt_file_yt_job_service.cpp
)

PEERDIR(
    library/cpp/yson
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/interface
    contrib/ydb/library/yql/providers/yt/lib/yson_helpers
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
