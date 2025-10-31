UNITTEST_FOR(cloud/filestore/libs/storage/service)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCS(
    helpers_ut.cpp
    service_ut.cpp
    service_ut_parentless.cpp
    service_ut_sharding.cpp
    service_actor_actions_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
