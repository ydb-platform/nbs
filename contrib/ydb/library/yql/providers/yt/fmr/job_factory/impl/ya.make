LIBRARY()

SRCS(
    yql_yt_job_factory_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    library/cpp/yson/node
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/interface
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
