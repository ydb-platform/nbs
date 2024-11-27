LIBRARY()

SRCS(
    config.cpp
    listener.cpp
)

PEERDIR(
    cloud/filestore/libs/endpoint
    cloud/filestore/libs/service
    cloud/filestore/libs/client
    cloud/filestore/libs/vfs

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/xml/document

    cloud/contrib/vhost
)

END()
