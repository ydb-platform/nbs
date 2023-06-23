UNITTEST_FOR(cloud/blockstore/libs/service_kikimr)

SRCS(
    auth_provider_kikimr_ut.cpp
    kikimr_test_env.cpp
    service_kikimr_ut.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/testlib/basics
)

REQUIREMENTS(ram:9)

END()
