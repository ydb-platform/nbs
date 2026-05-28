LIBRARY()


SRCS(
    client.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/server/protos

    contrib/libs/silk/src/fibers
)

END()
