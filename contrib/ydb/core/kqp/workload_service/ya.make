LIBRARY()

SRCS(
    kqp_has_full_scan_matcher.cpp
    kqp_has_path_matcher.cpp
    kqp_query_classifier.cpp
    kqp_workload_service.cpp
)

PEERDIR(
    contrib/ydb/core/cms/console

    contrib/ydb/core/fq/libs/compute/common

    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    contrib/ydb/core/kqp/query_data
    contrib/ydb/core/kqp/workload_service/actors

    contrib/ydb/core/mind

    contrib/ydb/core/resource_pools

    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/aclib
    contrib/ydb/library/yql/providers/pq/proto

    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    tables
)

RECURSE_FOR_TESTS(
    ut
)
