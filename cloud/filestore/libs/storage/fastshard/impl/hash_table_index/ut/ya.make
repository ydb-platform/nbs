GTEST()

SRCS(
    ../shard_ut.cpp
    ../shard_ut_error.cpp
    ../shard_ut_layout.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/impl/factory
    cloud/filestore/libs/storage/fastshard/impl/hash_table_index
    cloud/filestore/libs/storage/fastshard/storage_group

    cloud/fastshard/sn/impl
    cloud/fastshard/sn/server
    cloud/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest

    library/cpp/json
)

# The silk crash dumper sources its gdb scripts from the source tree
# (see SetUpCrashDumperScriptDir in cloud/fastshard/testlib).
DATA(arcadia/contrib/libs/silk/src/gdb)

END()
