UNITTEST_FOR(cloud/blockstore/libs/diagnostics)

PEERDIR(
    cloud/storage/core/libs/common

    library/cpp/eventlog/dumper
)

SRCS(
    cgroup_stats_fetcher_ut.cpp
    block_digest_ut.cpp
    fault_injection_ut.cpp
    postpone_time_predictor_ut.cpp
    profile_log_ut.cpp
    quota_metrics_ut.cpp
    request_stats_ut.cpp
    server_stats_ut.cpp
    stats_aggregator_ut.cpp
    user_counter_ut.cpp
    volume_perf_ut.cpp
    volume_stats_ut.cpp
)

END()
