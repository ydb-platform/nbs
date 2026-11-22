LIBRARY()

SRCS(
    helper.cpp
    protocol.cpp
    server.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
    cloud/storage/core/libs/rdma/impl
    cloud/storage/core/protos

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
