GTEST()

SRCS(
    ../loadtest_ut.cpp
)

PEERDIR(
    cloud/fastshard/loadtest/lib
    cloud/fastshard/protos
    cloud/fastshard/sn/iface
    cloud/fastshard/testlib
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/restricted/googletest/googletest
)

END()
