UNITTEST_FOR(cloud/blockstore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/large.inc)

SRCS(
    storage_aio_ut_large.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio
)

END()
