LIBRARY()

SRCS(
    keyring.cpp
    keyring_endpoints.cpp
    keyring_endpoints_test.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/protobuf/util
)

END()

RECURSE_FOR_TESTS(ut)
RECURSE_FOR_TESTS(ut/bin)