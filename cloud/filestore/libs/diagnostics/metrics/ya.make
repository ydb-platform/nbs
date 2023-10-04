LIBRARY()

GENERATE_ENUM_SERIALIZATION(public.h)

SRCS(
    aggregator.cpp
    histogram.h
    key.cpp
    label.cpp
    metric.cpp
    operations.cpp
    registry.cpp
    service.cpp
    visitor.cpp
    window_calculator.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_stress
)
