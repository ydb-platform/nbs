LIBRARY()

SRCS(
    config.cpp
    convert.cpp
    fsync_queue.cpp
    loop.cpp
    probes.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    tests
    ut
)


