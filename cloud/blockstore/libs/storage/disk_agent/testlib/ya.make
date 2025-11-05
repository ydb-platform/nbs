LIBRARY()

SRCS(
    test_env.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent

    cloud/storage/core/libs/aio

    contrib/ydb/library/actors/core
    library/cpp/testing/unittest

    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()
