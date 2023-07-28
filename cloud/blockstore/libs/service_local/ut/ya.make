UNITTEST_FOR(cloud/blockstore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    compound_storage_ut.cpp
    storage_aio_ut.cpp
    storage_null_ut.cpp
    storage_spdk_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/server

    cloud/storage/core/libs/aio
)

END()
