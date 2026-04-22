UNITTEST_FOR(cloud/storage/core/libs/grpc)

SRCS(
    executor_ut.cpp
    init_ut.cpp
    shutdown_ut.cpp
    utils_ut.cpp
)

ADDINCL(
    contrib/libs/grpc
)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
)

END()
