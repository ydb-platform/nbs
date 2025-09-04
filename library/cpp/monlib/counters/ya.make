LIBRARY()


SRCS(
    counters.cpp
    histogram.cpp
    meter.cpp
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
