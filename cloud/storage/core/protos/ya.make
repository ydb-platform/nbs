PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    certificate.proto
    endpoints.proto
    error.proto
    media.proto
    request_source.proto
    tablet.proto
    trace.proto
)

END()
