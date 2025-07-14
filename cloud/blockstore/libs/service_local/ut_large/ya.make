UNITTEST_FOR(cloud/blockstore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

SRCS(
    storage_local_ut_large.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio
)

END()
