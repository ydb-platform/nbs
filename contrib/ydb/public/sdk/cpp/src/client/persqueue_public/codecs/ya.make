LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    codecs.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
)

END()
