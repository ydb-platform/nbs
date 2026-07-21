UNITTEST_FOR(contrib/ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/blobstorage/ddisk
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/util/actorsys_test
)

SRCS(
    ddisk_actor_ut.cpp
    ddisk_actor_batch_write_ut.cpp
    ddisk_actor_pdisk_ut.cpp
    ddisk_sync_ut.cpp
    persistent_buffer_barriers_manager_ut.cpp
    persistent_buffer_space_allocator_ut.cpp
    segment_manager_ut.cpp
)

END()
