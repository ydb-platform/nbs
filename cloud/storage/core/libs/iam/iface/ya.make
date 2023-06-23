LIBRARY()

SRCS(
    client.cpp
    config.cpp
)

PEERDIR(
    cloud/storage/core/config
    cloud/storage/core/libs/common

    library/cpp/monlib/service/pages
    library/cpp/threading/future
)

END()
