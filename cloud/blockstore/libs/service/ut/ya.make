UNITTEST_FOR(cloud/blockstore/libs/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    device_handler_ut.cpp
    service_filtered_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
)

END()
