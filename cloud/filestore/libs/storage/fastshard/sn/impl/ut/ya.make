GTEST()

SRCS(
    ../storage_node_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/iface
    cloud/filestore/libs/storage/fastshard/sn/impl
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
