UNITTEST_FOR(cloud/filestore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

PEERDIR(
    cloud/storage/core/libs/aio
)

SRCS(
    index_ut.cpp
    service_ut.cpp
)

END()
