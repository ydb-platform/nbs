UNITTEST_FOR(cloud/filestore/libs/diagnostics/metrics)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    aggregator_ut.cpp
    histogram_ut.cpp
    key_ut.cpp
    label_ut.cpp
    metric_ut.cpp
    operations_ut.cpp
    registry_ut.cpp
    service_ut.cpp
    visitor_ut.cpp
    window_calculator_ut.cpp
)

END()
