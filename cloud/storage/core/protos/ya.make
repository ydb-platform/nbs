PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    authorization_mode.proto
    certificate.proto
    endpoints.proto
    error.proto
    media.proto
    request_source.proto
    tablet.proto
    throttler.proto
    trace.proto
    ydb_config_dispatcher_settings.proto
)

END()
