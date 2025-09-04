UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/skeleton)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/skeleton
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/actors
)

SRCS(
    skeleton_oos_logic_ut.cpp
    skeleton_vpatch_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
