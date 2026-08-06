PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/testing/benchmark/main
)

END()

RECURSE_FOR_TESTS(
    bench
)
