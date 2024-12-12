LIBRARY()

SRCS(
    validate.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib
)

END()

RECURSE_FOR_TESTS(ut)
