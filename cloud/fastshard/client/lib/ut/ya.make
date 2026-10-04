GTEST()

SRCS(
    ../command_ut.cpp
)

PEERDIR(
    cloud/fastshard/client/lib
    cloud/fastshard/protos
    cloud/fastshard/sn/iface
    cloud/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
