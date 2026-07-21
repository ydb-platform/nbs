LIBRARY()

SRCS(
    yql_yt_job_fmr.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/request_options/proto_helpers
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/file
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/interface
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/static
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/peer_tracker
    contrib/ydb/library/yql/providers/yt/fmr/tvm/impl
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/hasher
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    contrib/ydb/library/yql/providers/yt/job
)

YQL_LAST_ABI_VERSION()

END()
