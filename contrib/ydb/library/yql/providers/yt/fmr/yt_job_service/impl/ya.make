LIBRARY()

SRCS(
    yql_yt_job_service_impl.cpp
    yql_yt_table_write_distributed_session.cpp
)

PEERDIR(
    library/cpp/yt/error
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/interface
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
