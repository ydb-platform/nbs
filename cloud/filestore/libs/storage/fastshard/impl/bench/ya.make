G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    hash_table_index_bench.cpp
    null_storage_group.cpp
    shard_bench.cpp
)

PEERDIR(
    cloud/fastshard/bootstrap
    cloud/fastshard/testlib

    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/factory
    cloud/filestore/libs/storage/fastshard/impl/hash_table_index
    cloud/filestore/libs/storage/fastshard/storage_group
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future

    contrib/libs/silk/src/fibers
)

END()
