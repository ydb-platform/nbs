UNITTEST_FOR(cloud/filestore/libs/endpoint)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    service_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/endpoints/fs
)

END()
