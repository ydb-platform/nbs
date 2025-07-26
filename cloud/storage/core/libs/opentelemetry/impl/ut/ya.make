UNITTEST_FOR(cloud/storage/core/libs/opentelemetry/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    trace_service_client_ut.cpp
    trace_ut.cpp
)

END()
