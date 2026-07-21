UNITTEST_FOR(contrib/ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

SIZE(LARGE)

TAG(ya:fat)

PEERDIR(
    contrib/ydb/core/blobstorage/ddisk
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/util/actorsys_test
)

SRCS(
    ddisk_actor_pdisk_large_ut.cpp
    ddisk_actor_pdisk_sync_ut.cpp
)

END()
