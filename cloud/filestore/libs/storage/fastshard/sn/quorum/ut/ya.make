GTEST()

SRCS(
    ../storage_group_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/libs/storage/fastshard/testlib

    contrib/restricted/googletest/googletest
)

END()
