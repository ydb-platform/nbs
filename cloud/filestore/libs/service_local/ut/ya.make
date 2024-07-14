UNITTEST_FOR(cloud/filestore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    service_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio
)

END()
