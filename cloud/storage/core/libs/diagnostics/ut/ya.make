UNITTEST_FOR(cloud/storage/core/libs/diagnostics)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/json
)

SRCS(
    cgroup_stats_fetcher_ut.cpp
    histogram_types_ut.cpp
    logging_ut.cpp
    max_calculator_ut.cpp
    postpone_time_predictor_ut.cpp
    request_counters_ut.cpp
    solomon_counters_ut.cpp
    trace_processor_mon_ut.cpp
    trace_serializer_ut.cpp
    weighted_percentile_ut.cpp
)

END()
