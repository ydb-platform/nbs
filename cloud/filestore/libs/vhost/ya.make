LIBRARY()

SRCS(
    client.cpp
    request.cpp
    server.cpp
)

PEERDIR(
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/vhost-client

    library/cpp/threading/future

    cloud/contrib/vhost

    contrib/libs/linux-headers
)


END()
