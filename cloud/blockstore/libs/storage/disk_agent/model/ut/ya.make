UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    bandwidth_calculator_ut.cpp
    compare_configs_ut.cpp
    chaos_storage_provider_ut.cpp
    device_client_ut.cpp
    device_generator_ut.cpp
    device_guard_ut.cpp
    device_scanner_ut.cpp
)

END()
