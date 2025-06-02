LIBRARY()

SRCS(
    busy_idle_calculator.cpp
    counters_helper.cpp
    cgroup_stats_fetcher.cpp
    critical_events.cpp
    executor_counters.cpp
    histogram_types.cpp
    histogram.cpp
    incomplete_request_processor.cpp
    logging.cpp
    max_calculator.cpp
    monitoring.cpp
    postpone_time_predictor.cpp
    request_counters.cpp
    solomon_counters.cpp
    stats_fetcher.cpp
    stats_updater.cpp
    task_stats_fetcher.cpp
    trace_processor_mon.cpp
    trace_processor.cpp
    trace_reader.cpp
    trace_serializer.cpp
    weighted_percentile.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/netlink
    cloud/storage/core/protos

    library/cpp/lwtrace/mon

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/prof
    library/cpp/containers/ring_buffer
    library/cpp/deprecated/atomic
    library/cpp/histogram/hdr
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    library/cpp/monlib/service/pages/tablesorter

    logbroker/unified_agent/client/cpp/logger
)

END()

RECURSE_FOR_TESTS(qemu_ut)
RECURSE_FOR_TESTS(ut)
