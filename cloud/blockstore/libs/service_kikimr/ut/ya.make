UNITTEST_FOR(cloud/blockstore/libs/service_kikimr)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    auth_provider_kikimr_ut.cpp
    kikimr_test_env.cpp
    service_kikimr_ut.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/testlib/basics
)

END()
