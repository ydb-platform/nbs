LIBRARY()

SRCS(
    vhost-user-protocol/message.cpp
    vhost-client.cpp
    vhost-queue.cpp

    chunked_allocator.cpp
    monotonic_buffer_resource.cpp
    vhost-buffered-client.cpp
)

PEERDIR(
    library/cpp/logger
)

ADDINCL(
    cloud/contrib/vhost
)

END()
