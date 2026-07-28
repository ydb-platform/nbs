PROTO_LIBRARY()

    PEERDIR(
        contrib/proto/opentelemetry
    )

    EXCLUDE_TAGS(
        GO_PROTO
        JAVA_PROTO
    )

END()
