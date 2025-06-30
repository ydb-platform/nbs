UNITTEST_FOR(cloud/blockstore/libs/opentelemetry/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    trace_ut.cpp
)

END()
