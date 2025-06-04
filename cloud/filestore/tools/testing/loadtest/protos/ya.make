PROTO_LIBRARY(filestore-testing-loadtest-protos)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    cloud/filestore/public/api/protos
)

SRCS(
    loadtest.proto
)

END()
