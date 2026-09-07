LIBRARY()

SRCS(
    factory.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/mem
    cloud/filestore/libs/storage/fastshard/impl/naive_mirrored

    cloud/filestore/private/api/unsafe_protos

    contrib/libs/silk/src/fibers
)

END()
