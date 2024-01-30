LIBRARY()

SRCS(
    client_storage.cpp
    endpoint_poller.cpp
    socket_poller.cpp
    uds_socket_client.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/protos
    contrib/libs/grpc
)

END()
