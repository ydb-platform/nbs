UNITTEST_FOR(cloud/blockstore/libs/rdma/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    buffer_ut.cpp
    client_ut.cpp
    server_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

END()
