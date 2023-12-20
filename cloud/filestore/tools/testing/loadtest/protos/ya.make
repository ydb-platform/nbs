PROTO_LIBRARY(filestore-testing-loadtest-protos)

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    cloud/filestore/public/api/protos
)

SRCS(
    loadtest.proto
)

END()
