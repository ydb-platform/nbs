LIBRARY()

SRCS(
    helper.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api

    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
    cloud/storage/core/libs/rdma/impl
    cloud/storage/core/protos
)

END()
