LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/operation_id/protos
    library/cpp/cgiparam
    library/cpp/uri
)

END()
