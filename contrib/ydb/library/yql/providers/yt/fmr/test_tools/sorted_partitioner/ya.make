LIBRARY()

SRCS(
    yql_yt_sorted_partitioner_test_tools.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/partitioner
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/fmr_block_iterator
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
)

YQL_LAST_ABI_VERSION()

END()

