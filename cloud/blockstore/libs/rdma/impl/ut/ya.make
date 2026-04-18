GTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../buffer_ut.cpp
    ../client_ut.cpp
    ../list_ut.cpp
    ../poll_ut.cpp
    ../rcu_ut.cpp
    ../server_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/public/api/protos
)

END()
