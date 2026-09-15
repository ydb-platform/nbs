GTEST()

SRCS(
    ../command_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/fastshard_client/lib
    cloud/filestore/libs/storage/fastshard/sn/iface
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
