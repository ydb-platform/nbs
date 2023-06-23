PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)

EXCLUDE_TAGS(PY_PROTO PY3_PROTO)

SRCS(
    auth.proto
    ic.proto
    log.proto
    shared_cache.proto
    sys.proto
)

END()
