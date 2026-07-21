UNITTEST()

SRCS(
    yql_yt_job_factory_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/interface
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/impl
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
    contrib/ydb/library/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
