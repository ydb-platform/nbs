PROTO_LIBRARY(state-file-tool-protos)

ONLY_TAGS(CPP_PROTO)

SRCS(
    state_file_tool.proto
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/protos
    cloud/filestore/public/api/protos
)

END()
