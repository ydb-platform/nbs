UNITTEST_FOR(cloud/filestore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

PEERDIR(
    cloud/storage/core/libs/aio
)

SRCS(
    service_ut_stress.cpp
)

END()
