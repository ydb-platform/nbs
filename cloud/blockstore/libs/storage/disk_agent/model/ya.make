LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    bandwidth_calculator.cpp
    compare_configs.cpp
    config.cpp
    device_client.cpp
    device_generator.cpp
    device_guard.cpp
    device_scanner.cpp
    probes.cpp
    request_parser.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    contrib/ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
