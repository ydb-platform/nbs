GTEST()

SRCS(
    ../server_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/ipc
    cloud/filestore/libs/storage/fastshard/sn/iface
    cloud/filestore/libs/storage/fastshard/sn/server

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
