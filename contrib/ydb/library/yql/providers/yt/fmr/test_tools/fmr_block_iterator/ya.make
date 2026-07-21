LIBRARY()

SRCS(
    yql_yt_fmr_block_iterator.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/yson
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
)

YQL_LAST_ABI_VERSION()

END()

