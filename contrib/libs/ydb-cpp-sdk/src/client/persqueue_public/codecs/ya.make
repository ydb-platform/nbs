LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    codecs.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/topic/codecs
)

END()
