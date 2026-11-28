LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    discovery_mutator.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/extension_common
)

END()
