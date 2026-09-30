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

# The silk crash dumper sources its gdb scripts from the source tree
# (see SetUpCrashDumperScriptDir in cloud/fastshard/testlib).
DATA(arcadia/contrib/libs/silk/src/gdb)

END()
