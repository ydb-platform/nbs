UNITTEST_FOR(cloud/filestore/apps/client/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    aggregate_ut.cpp
    command_ut.cpp
    performance_profile_params_ut.cpp
)

END()
