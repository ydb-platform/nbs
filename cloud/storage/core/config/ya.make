PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    features.proto
    grpc_client.proto
    iam.proto
    opentelemetry_client.proto
)

END()
