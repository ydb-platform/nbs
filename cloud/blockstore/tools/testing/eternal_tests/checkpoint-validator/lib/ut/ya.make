UNITTEST_FOR(
    cloud/blockstore/tools/testing/eternal_tests/checkpoint-validator/lib
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/checkpoint-validator/lib
)

SRCS(
    validator_ut.cpp
)

END()
