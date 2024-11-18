UNITTEST_FOR(cloud/blockstore/libs/endpoints)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    endpoint_manager_ut.cpp
    service_endpoint_ut.cpp
    session_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/endpoints_grpc
    cloud/blockstore/libs/server
    library/cpp/testing/gmock_in_unittest
)

END()
