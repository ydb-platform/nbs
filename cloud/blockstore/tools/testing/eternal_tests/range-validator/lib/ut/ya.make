UNITTEST_FOR(cloud/blockstore/tools/testing/eternal_tests/range-validator/lib)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    validate_ut.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib
    cloud/blockstore/tools/testing/eternal_tests/range-validator/lib
)

END()
