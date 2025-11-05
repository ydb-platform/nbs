UNITTEST_FOR(cloud/filestore/libs/service_kikimr)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    auth_provider_kikimr_ut.cpp
    kikimr_test_env.cpp
    service_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/basics
)

END()
