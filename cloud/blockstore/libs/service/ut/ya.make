UNITTEST_FOR(cloud/blockstore/libs/service)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    context_ut.cpp
    device_handler_ut.cpp
    service_filtered_ut.cpp
    storage_ut.cpp
)

END()
