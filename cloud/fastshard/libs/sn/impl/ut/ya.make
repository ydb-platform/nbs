GTEST()

SRCS(
    ../storage_node_ut.cpp
)

PEERDIR(
    cloud/fastshard/libs/common
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/impl
    cloud/fastshard/libs/sn/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

# The silk crash dumper sources its gdb scripts from the source tree
# (see SetUpCrashDumperScriptDir in cloud/fastshard/libs/sn/testlib).
DATA(arcadia/contrib/libs/silk/src/gdb)

END()
