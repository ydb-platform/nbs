UNITTEST_FOR(cloud/blockstore/libs/storage/cell_proxy)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    cell_proxy_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/service
    contrib/ydb/library/actors/testlib
)

END()
