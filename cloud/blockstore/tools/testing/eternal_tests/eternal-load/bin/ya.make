PROGRAM(eternal-load)

SRCS(
    app.cpp
    main.cpp
    options.cpp
    test.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib

    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/threading/future
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    tests
)
