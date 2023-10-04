UNITTEST_FOR(cloud/filestore/libs/storage/service)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)
ENDIF()

SRCS(
    helpers_ut.cpp
    service_ut.cpp
    service_actor_actions_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
