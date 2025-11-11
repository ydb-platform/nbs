LIBRARY()

SRCS(
    block_digest.cpp
    config.cpp
    critical_events.cpp
    diag_down_graph.cpp
    downtime_history.cpp
    dumpable.cpp
    fault_injection.cpp
    incomplete_request_processor.cpp
    incomplete_requests.cpp
    hostname.cpp
    probes.cpp
    profile_log.cpp
    quota_metrics.cpp
    request_stats.cpp
    server_stats.cpp
    server_stats_test.cpp
    stats_aggregator.cpp
    stats_helpers.cpp
    user_counter.cpp
    volume_balancer_switch.cpp
    volume_perf.cpp
    volume_stats.cpp
    volume_stats_test.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics/data
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/throttling
    cloud/storage/core/libs/user_stats/counter

    library/cpp/deprecated/atomic
    library/cpp/digest/crc32c
    library/cpp/eventlog
    library/cpp/histogram/hdr
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/encode/spack
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    library/cpp/monlib/service/pages/tablesorter
    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(
    gtest
    ut
)
