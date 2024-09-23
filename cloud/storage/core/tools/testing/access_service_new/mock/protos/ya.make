PROTO_LIBRARY()

GRPC()
ONLY_TAGS(GO_PROTO)

SRCS(
    access_service.proto
    access.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/status
)
END()
