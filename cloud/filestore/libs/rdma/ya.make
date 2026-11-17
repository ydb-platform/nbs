LIBRARY()

SRCS(
    protocol.cpp
    server.cpp
)

PEERDIR(
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
