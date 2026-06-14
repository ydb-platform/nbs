LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    discovery_mutator.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/extension_common
)

END()
