UNITTEST_FOR(cloud/blockstore/libs/rdma/fake)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    client_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/testlib

    ydb/core/testlib
    ydb/core/testlib/basics
    ydb/library/actors/core

    library/cpp/testing/unittest
)

END()
