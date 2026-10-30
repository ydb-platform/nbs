GTEST()

SRCS(
    ../command_ut.cpp
)

PEERDIR(
    cloud/fastshard/apps/client/lib
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/testlib
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
