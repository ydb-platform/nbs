UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCS(
    tablet_ut_cache_stress.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

ENV(SANITIZER_TYPE=${SANITIZER_TYPE})

YQL_LAST_ABI_VERSION()

END()
