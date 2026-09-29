GTEST()

SRCS(
    ../server_ut.cpp
)

PEERDIR(
    cloud/fastshard/ipc
    cloud/fastshard/sn/iface
    cloud/fastshard/sn/server
    cloud/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
