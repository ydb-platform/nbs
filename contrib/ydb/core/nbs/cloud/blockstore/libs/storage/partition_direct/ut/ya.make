UNITTEST_FOR(contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct)

SRCS(
    base_test_fixture.cpp
    ddisk_data_copier_ut.cpp
    direct_block_group_impl_ut.cpp
    direct_block_group_mock.cpp
    direct_block_group_test_fixture.cpp
    erase_request_ut.cpp
    flush_request_ut.cpp
    read_request_ut.cpp
    vchunk_ut.cpp
    write_request_test_fixture.cpp
    write_request_ut.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    contrib/ydb/core/protos
    contrib/ydb/core/testlib
)

END()
