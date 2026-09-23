GTEST()

SRCS(
    ../loadtest_ut.cpp
)

PEERDIR(
    cloud/fastshard/apps/loadtest/lib
    cloud/fastshard/public/api/protos
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/testlib
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/restricted/googletest/googletest
)

END()
