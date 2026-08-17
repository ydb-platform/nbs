UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/synclog
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/keys
    contrib/ydb/library/pdisk_io
)

SRCS(
    synclog_real_pdisk_ut.cpp
)

END()
