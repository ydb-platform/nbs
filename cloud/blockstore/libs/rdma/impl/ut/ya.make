UNITTEST_FOR(cloud/blockstore/libs/rdma/impl)

SRCS(
    buffer_ut.cpp
    client_ut.cpp
    server_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

END()
