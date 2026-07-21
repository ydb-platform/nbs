PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    contrib/ydb/library/yql/providers/common/proto
)

SRCS(
    mock.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()
