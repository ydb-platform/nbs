UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ENDIF()

SPLIT_FACTOR(1)

SRCS(
    tablet_ut_data_stress_large.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

ENV(SANITIZER_TYPE=${SANITIZER_TYPE})

YQL_LAST_ABI_VERSION()

END()
