UNITTEST_FOR(cloud/blockstore/libs/rdma/iface)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    protobuf_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

END()
