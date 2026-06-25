UNITTEST_FOR(cloud/blockstore/libs/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

IF (SANITIZER_TYPE == "memory")
    SIZE(MEDIUM)
    TIMEOUT(300)
ENDIF()

SRCS(
    blocks_info_ut.cpp
    context_ut.cpp
    device_handler_ut.cpp
    overlapping_requests_guard_service_ut.cpp
    service_filtered_ut.cpp
    split_request_service_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
)

END()
