LIBRARY()

SRCS(
    aligned_test_scenario.cpp
    config.cpp
    test_executor.cpp
    unaligned_test_scenario.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config

    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/io_uring

    library/cpp/aio
    library/cpp/deprecated/atomic
    library/cpp/digest/crc32c
    library/cpp/protobuf/json
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(ut)
