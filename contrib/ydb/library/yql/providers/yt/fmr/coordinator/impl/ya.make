LIBRARY()

SRCS(
    yql_yt_coordinator_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/resource
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl
    contrib/ydb/library/yql/providers/yt/fmr/gc_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/failure_injector
    contrib/ydb/library/yql/utils/log
)

RESOURCE(
    default_operation_settings.yson default_operation_settings.yson
    default_coordinator_settings.yson default_coordinator_settings.yson
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
