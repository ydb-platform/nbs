LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    iam.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/iam/common
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    library/cpp/threading/future
)

END()
