GTEST()

SRCS(
    ../storage_group_journalled_ut.cpp
    ../storage_group_ut.cpp
)

PEERDIR(
    cloud/fastshard/testlib
    cloud/filestore/libs/storage/fastshard/sn/quorum
    
    contrib/restricted/googletest/googletest
)

END()
