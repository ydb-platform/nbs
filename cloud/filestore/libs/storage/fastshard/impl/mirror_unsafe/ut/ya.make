GTEST()

SRCS(
    ../shard_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/impl/mirror_unsafe
    cloud/filestore/libs/storage/fastshard/sn/impl
    cloud/filestore/libs/storage/fastshard/sn/server
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
