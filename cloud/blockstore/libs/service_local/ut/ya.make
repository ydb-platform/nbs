UNITTEST_FOR(cloud/blockstore/libs/service_local)

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

REQUIREMENTS(
    ram_disk:1
)

END()
