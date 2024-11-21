UNITTEST_FOR(cloud/storage/core/libs/grpc)

SRCS(
    executor_ut.cpp
    init_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
)

END()
