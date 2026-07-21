LIBRARY()

SRCS(
    yql_yt_job_preparer_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/job_preparer/interface
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/interface
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/utils
    library/cpp/string_utils/quote
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
