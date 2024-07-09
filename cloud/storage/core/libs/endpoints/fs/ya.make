LIBRARY()

SRCS(
    fs_endpoints.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/protobuf/util
)

END()

RECURSE_FOR_TESTS(ut)
