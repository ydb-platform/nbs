PROTO_LIBRARY()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    events.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()