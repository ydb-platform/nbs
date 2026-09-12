LIBRARY()

SRCS(
    group_factory.cpp
    shard_factory.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/mem
    cloud/filestore/libs/storage/fastshard/impl/hash_table_index
    cloud/filestore/libs/storage/fastshard/sn/client
    cloud/filestore/libs/storage/fastshard/sn/quorum

    cloud/filestore/private/api/protos
)

END()
