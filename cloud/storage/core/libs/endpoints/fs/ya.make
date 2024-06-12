LIBRARY()

SRCS(
    fs_endpoints.cpp
    fs_endpoints_test.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/protobuf/util
)

END()

RECURSE_FOR_TESTS(ut)