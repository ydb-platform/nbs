LIBRARY()

SRCS(
    compute_client.cpp
    key_provider.cpp
    kms_client.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/encryption
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/iam/iface

    cloud/bitbucket/private-api/yandex/cloud/priv/kms/v1
    ydb/public/api/client/yc_private/compute/inner

    library/cpp/threading/future

    contrib/libs/grpc
)

END()

RECURSE(
    example
)

RECURSE_FOR_TESTS(
    ut
    ut_client
)
