UNITTEST_FOR(cloud/blockstore/libs/rdma/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    buffer_ut.cpp
    client_ut.cpp
    server_ut.cpp
    poll_ut.cpp
    list_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

END()
