UNITTEST_FOR(cloud/filestore/tools/analytics/profile_tool/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    common_filter_params_ut.cpp
    factory_ut.cpp
    mask_ut.cpp
)

END()
