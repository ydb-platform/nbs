UNITTEST_FOR(cloud/blockstore/libs/storage/ss_proxy)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    path_description_backup_ut.cpp
    ss_proxy_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
