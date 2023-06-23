PROGRAM(eternal-load)

SRCS(
    app.cpp
    main.cpp
    options.cpp
    test.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal-tests/eternal-load/lib

    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/threading/future
    library/cpp/json
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()

RECURSE_FOR_TESTS(
    tests
)
