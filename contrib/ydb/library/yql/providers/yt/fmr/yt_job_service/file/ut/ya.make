UNITTEST()

SRCS(
    yql_yt_file_yt_job_service_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
    contrib/ydb/library/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
