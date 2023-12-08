LIBRARY()

SRCS(
    config.cpp
    listener.cpp
    server.cpp
)

PEERDIR(
    cloud/filestore/libs/endpoint
    cloud/filestore/libs/service
    cloud/filestore/libs/vfs

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    cloud/contrib/vhost
)

END()
