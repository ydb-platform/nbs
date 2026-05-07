LIBRARY()

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    schema.h
    schema.cpp
    tx_ack_timeout.cpp
    tx_aggr_stat_response.cpp
    tx_analyze.cpp
    tx_analyze_deadline.cpp
    tx_analyze_table_delivery_problem.cpp
    tx_analyze_table_request.cpp
    tx_analyze_table_response.cpp
    tx_configure.cpp
    tx_datashard_scan_response.cpp
    tx_finish_trasersal.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_navigate.cpp
    tx_resolve.cpp
    tx_response_tablet_distribution.cpp
    tx_schedule_traversal.cpp
    tx_schemeshard_stats.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/statistics/database
    yql/essentials/core/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
