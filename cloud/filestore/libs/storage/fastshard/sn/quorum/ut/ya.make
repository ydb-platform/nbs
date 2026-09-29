GTEST()

SRCS(
    ../storage_group_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/quorum

    cloud/fastshard/testlib

    contrib/restricted/googletest/googletest
)

END()
