LIBRARY()

SRCS(
    yql_yt_fmr_partitioner.cpp
    yql_yt_ordered_partitioner.cpp
    yql_yt_reduce_partitioner.cpp
    yql_yt_sorted_partitioner.cpp
    yql_yt_sorted_partitioner_base.cpp
)

PEERDIR(
    library/cpp/iterator
    library/cpp/yson/node
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
