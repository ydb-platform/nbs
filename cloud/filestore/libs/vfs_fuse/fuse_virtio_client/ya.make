LIBRARY()

SRCS(
    fuse_virtio_client.cpp
    request.cpp
)

PEERDIR(
    cloud/filestore/public/api/protos
    cloud/storage/core/libs/vhost-client
)


END()
