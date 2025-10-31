LIBRARY()

SRCS(
    test_env.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent

    cloud/storage/core/libs/aio

    ydb/library/actors/core
    library/cpp/testing/unittest

    ydb/core/mind/bscontroller
    ydb/core/tablet_flat
    ydb/core/testlib
    ydb/core/testlib/basics
)

END()
