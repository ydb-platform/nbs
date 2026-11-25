LIBRARY()

SRCS(
    storage_group_factory.cpp
)

PEERDIR(
    cloud/filestore/private/api/protos

    cloud/fastshard/libs/storage_group
)

END()
