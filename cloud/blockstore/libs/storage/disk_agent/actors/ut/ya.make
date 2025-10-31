UNITTEST_FOR(cloud/blockstore/libs/storage/disk_agent/actors)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    session_cache_actor_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr

    ydb/library/actors/testlib
)

END()
