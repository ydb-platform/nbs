UNITTEST_FOR(cloud/storage/core/libs/endpoints/fs)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    fs_endpoints_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/endpoints/fs
)

END()
