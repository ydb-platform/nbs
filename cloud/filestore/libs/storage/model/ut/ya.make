UNITTEST_FOR(cloud/filestore/libs/storage/model)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    block_buffer_ut.cpp
    range_ut.cpp
    utils_ut.cpp
)

END()

RECURSE_FOR_TESTS(ut)
