GTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../protobuf_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/public/api/protos
)

END()
