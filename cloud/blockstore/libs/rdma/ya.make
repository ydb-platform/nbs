LIBRARY()

SRCS(
    helper.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
    cloud/storage/core/libs/rdma/impl
)

END()

RECURSE(
    fake
)
