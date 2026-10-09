LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    extension.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/libs/ydb-cpp-sdk/src/client/driver
)

END()
