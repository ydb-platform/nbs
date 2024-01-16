LIBRARY()

SRCS(
    compute_client.cpp
    kms_client.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/kms/iface
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/common

    ydb/public/api/client/yc_private/kms
    ydb/public/api/client/yc_private/compute/inner

    library/cpp/threading/future

    contrib/libs/grpc
)

END()

RECURSE(
    example
)
