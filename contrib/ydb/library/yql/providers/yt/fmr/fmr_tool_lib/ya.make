LIBRARY()

SRCS(
    yql_yt_fmr_initializer.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/gateway/fmr
    contrib/ydb/library/yql/providers/yt/gateway/lib
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/client
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/impl
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/file/metadata/impl
    contrib/ydb/library/yql/providers/yt/fmr/file/upload/impl
    contrib/ydb/library/yql/providers/yt/fmr/gc_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_preparer/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/impl
    contrib/ydb/library/yql/providers/yt/fmr/worker/impl
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/impl

    contrib/ydb/library/yql/providers/common/proto
)

YQL_LAST_ABI_VERSION()

END()
