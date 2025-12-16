LIBRARY()

SRCS(
    config.cpp
    helpers.cpp
    listener.cpp
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

RECURSE_FOR_TESTS(ut)
