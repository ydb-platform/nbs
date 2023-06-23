UNITTEST_FOR(cloud/blockstore/libs/service_local)

TAG(
    ya:fat
    sb:ssd
)

SIZE(LARGE)

SRCS(
    storage_aio_ut_large.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio
)

END()
