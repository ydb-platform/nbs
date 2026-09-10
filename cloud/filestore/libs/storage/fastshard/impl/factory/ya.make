LIBRARY()

SRCS(
    factory.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/mem
    cloud/filestore/libs/storage/fastshard/impl/hash_table_index

    cloud/filestore/private/api/protos
)

END()
