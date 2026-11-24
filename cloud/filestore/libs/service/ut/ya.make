UNITTEST_FOR(cloud/filestore/libs/service)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    mediakind_ut.cpp
    request_ut.cpp
    service_actions_ut.cpp
)

END()
