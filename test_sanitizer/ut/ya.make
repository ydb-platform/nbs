UNITTEST_FOR(test_sanitizer)

ENV(MSAN_OPTIONS=max_allocation_size_mb=1)

SRCS(
    test_ut.cpp
)

END()
