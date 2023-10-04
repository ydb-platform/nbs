UNITTEST_FOR(cloud/filestore/libs/storage/ss_proxy)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)
ENDIF()

SRCS(
    ss_proxy_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
