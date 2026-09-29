GTEST()

SRCS(
    ../storage_node_ut.cpp
)

PEERDIR(
    cloud/fastshard/sn/iface
    cloud/fastshard/sn/impl
    cloud/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
