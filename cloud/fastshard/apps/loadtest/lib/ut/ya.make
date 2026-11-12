GTEST()

SRCS(
    ../loadtest_ut.cpp
)

PEERDIR(
    cloud/fastshard/apps/loadtest/lib
    cloud/fastshard/libs/sn/client
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/testlib
    cloud/fastshard/public/api/protos
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/restricted/googletest/googletest
)

END()
