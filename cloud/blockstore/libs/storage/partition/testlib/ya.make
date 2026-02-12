LIBRARY()

SRCS(
    part_client.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition
    contrib/ydb/library/actors/core
    library/cpp/testing/unittest
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()
