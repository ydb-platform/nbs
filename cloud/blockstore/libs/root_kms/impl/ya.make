LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/root_kms/iface

    cloud/storage/core/libs/common
    cloud/storage/core/libs/grpc

    library/cpp/threading/future

    ydb/public/api/client/yc_private/kms
)

END()

RECURSE_FOR_TESTS(
    ut
)
