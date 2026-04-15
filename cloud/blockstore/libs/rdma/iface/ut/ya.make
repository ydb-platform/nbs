GTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../configs_ut.cpp
    ../protobuf_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/public/api/protos
)

END()
