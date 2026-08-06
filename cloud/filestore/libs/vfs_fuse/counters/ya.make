LIBRARY()

SRCS(
    max_counter.cpp
    relaxed_counters.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
)
