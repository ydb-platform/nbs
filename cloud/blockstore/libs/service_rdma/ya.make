LIBRARY()

SRCS(
    rdma_protocol.cpp
    rdma_target.cpp
)

PEERDIR(
    cloud/blockstore/libs/service

    cloud/blockstore/libs/storage/protos
)

END()

RECURSE_FOR_TESTS(ut)
