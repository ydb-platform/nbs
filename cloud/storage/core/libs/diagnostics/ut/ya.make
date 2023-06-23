UNITTEST_FOR(cloud/storage/core/libs/diagnostics)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/json
)

SRCS(
    histogram_types_ut.cpp
    logging_ut.cpp
    max_calculator_ut.cpp
    request_counters_ut.cpp
    solomon_counters_ut.cpp
    trace_processor_ut.cpp
    trace_serializer_ut.cpp
    weighted_percentile_ut.cpp
)

END()
