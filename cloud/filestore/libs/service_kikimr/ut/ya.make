UNITTEST_FOR(cloud/filestore/libs/service_kikimr)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    kikimr_test_env.cpp
    service_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/testlib/basics
)

END()
