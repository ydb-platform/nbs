UNITTEST_FOR(cloud/blockstore/libs/rdma/iface)

SRCS(
    list_ut.cpp
    poll_ut.cpp
    protobuf_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
)

END()
