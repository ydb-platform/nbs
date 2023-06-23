UNITTEST_FOR(cloud/blockstore/libs/endpoints)

SRCS(
    endpoint_manager_ut.cpp
    service_endpoint_ut.cpp
    session_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/endpoints_grpc
    cloud/blockstore/libs/server
)

END()
