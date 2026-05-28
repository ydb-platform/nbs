LIBRARY()


SRCS(
    server.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/server/protos

    contrib/libs/silk/src/fibers

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
