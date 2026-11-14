LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    jwt.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/json
)

END()
