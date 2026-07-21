UNITTEST()

SRCS(
    yql_yt_worker_ut.cpp
    yql_yt_worker_status_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/impl
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_preparer/impl
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/table_data_service
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/worker/impl
)

YQL_LAST_ABI_VERSION()

END()
