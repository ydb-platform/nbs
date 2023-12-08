LIBRARY()

SRCS(
    config.cpp
    fsync_queue.cpp
    loop.cpp
    probes.cpp
)

PEERDIR(
    cloud/filestore/config
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    tests
    ut
)


