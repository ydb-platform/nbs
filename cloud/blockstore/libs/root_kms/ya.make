LIBRARY()

SRCS(
    key_provider.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/encryption

    library/cpp/string_utils/base64
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
