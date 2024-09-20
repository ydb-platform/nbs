PROTO_LIBRARY(filestore-testing-replay-protos)

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    cloud/filestore/public/api/protos
)

SRCS(
    replay.proto
)

END()
