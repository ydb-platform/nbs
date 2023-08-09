LIBRARY()

SRCS(
    compute_client.cpp
    key_provider.cpp
    kms_client.cpp
)

PEERDIR(
    cloud/blockstore/libs/encryption
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/iam/iface

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
