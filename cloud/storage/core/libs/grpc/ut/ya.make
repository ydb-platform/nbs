UNITTEST_FOR(cloud/storage/core/libs/grpc)

ADDINCL(
    contrib/libs/grpc
)

SRCS(
    executor_ut.cpp
    init_ut.cpp
    tls_certificate_provider_ut.cpp
    tls_utils_ut.cpp
    utils_ut.cpp
)

ADDINCL(
    contrib/libs/grpc
)

DATA(
    arcadia/cloud/storage/core/libs/grpc/ut/certs/ca.crt
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.crt
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.key
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server2.crt
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server2.key
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/gmock_in_unittest
)

END()
