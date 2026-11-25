UNITTEST_FOR(cloud/filestore/tools/analytics/profile_tool/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    command_ut.cpp
    common_filter_params_ut.cpp
    factory_ut.cpp
    mask_ut.cpp
    time_range_ut.cpp
)

END()
