PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    operation_id.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
