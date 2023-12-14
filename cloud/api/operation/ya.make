OWNER(g:cloud-nbs)

PROTO_LIBRARY()

GRPC()
ONLY_TAGS(GO_PROTO)

PROTO_NAMESPACE(cloud/api/operation)

SRCS(
    operation.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/code
    rpc/errdetails
    rpc/status
    type/timeofday
    type/dayofweek
)

END()
