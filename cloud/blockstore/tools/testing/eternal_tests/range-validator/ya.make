PROGRAM(range-validator)

SRCS(main.cpp)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib
    cloud/blockstore/tools/testing/eternal_tests/range-validator/lib

    library/cpp/getopt
)

END()

RECURSE(lib)
