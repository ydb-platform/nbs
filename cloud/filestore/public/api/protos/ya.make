PROTO_LIBRARY(filestore-public-api-protos)

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    action.proto
    checkpoint.proto
    cluster.proto
    const.proto
    data.proto
    endpoint.proto
    fs.proto
    headers.proto
    locks.proto
    node.proto
    noderefs.proto
    ping.proto
    session.proto
)

PEERDIR(
    cloud/storage/core/protos

    library/cpp/lwtrace/protos
)

END()
