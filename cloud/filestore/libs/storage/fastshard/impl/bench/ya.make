G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    delay_policy.cpp
    hash_table_index_bench.cpp
    null_storage_group.cpp
    shard_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/hash_table_index
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future

    contrib/libs/silk/src/fibers
)

END()
