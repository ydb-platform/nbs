GTEST()

SRCS(
    ../server_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/ipc
    cloud/filestore/libs/storage/fastshard/sn/iface
    cloud/filestore/libs/storage/fastshard/sn/server
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
