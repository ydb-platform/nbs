LIBRARY(blockstore-libs-storage-testlib-partition-lite)

SRCS(
    ../part_client.cpp
    ../test_runtime.cpp
    ../ut_helpers.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition
    cloud/blockstore/libs/storage/partition_common

    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/testlib
    library/cpp/testing/unittest
)

END()
