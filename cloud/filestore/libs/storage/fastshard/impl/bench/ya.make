G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    delay_policy.cpp
    naive_mirrored_bench.cpp
    null_storage_group.cpp
    shard_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/impl/naive_mirrored
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/private/api/unsafe_protos

    cloud/storage/core/libs/common

    library/cpp/threading/future

    contrib/libs/silk/src/fibers
)

END()
