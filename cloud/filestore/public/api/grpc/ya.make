PROTO_LIBRARY(filestore-public-api-grpc)

GRPC()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    service.proto
)

PEERDIR(
    cloud/filestore/public/api/protos
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

GO_GRPC_GATEWAY_SRCS(
    service.proto
)

END()
