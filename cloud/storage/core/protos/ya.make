PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    certificate.proto
    endpoints.proto
    error.proto
    media.proto
    tablet.proto
    trace.proto
)

END()
