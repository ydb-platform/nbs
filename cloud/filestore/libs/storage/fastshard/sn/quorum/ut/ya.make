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

# The silk crash dumper sources its gdb scripts from the source tree
# (see SetUpCrashDumperScriptDir in cloud/fastshard/testlib).
DATA(arcadia/contrib/libs/silk/src/gdb)

END()
