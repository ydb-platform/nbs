UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(30)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/util
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/dsproxy
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/testlib/default
)

SRCS(
    dsproxy_ut.cpp
)

END()
