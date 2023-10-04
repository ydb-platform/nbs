UNITTEST_FOR(cloud/filestore/libs/diagnostics/metrics)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCS(
    aggregator_ut_stress.cpp
    histogram_ut_stress.cpp
    metric_ut_stress.cpp
    operations_ut_stress.cpp
    registry_ut_stress.cpp
    utils.cpp
    window_calculator_ut_stress.cpp
)

END()
