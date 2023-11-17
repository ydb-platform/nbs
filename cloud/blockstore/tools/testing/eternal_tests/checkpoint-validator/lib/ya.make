LIBRARY()

SRCS(
    validator.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics

    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib
)

END()

RECURSE_FOR_TESTS(ut)
