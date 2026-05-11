GTEST()

LICENSE(Apache-2.0)

LICENSE_TEXTS(${ARCADIA_ROOT}/contrib/libs/silk/LICENSE)

NO_COMPILER_WARNINGS()

CXXFLAGS(-std=c++20)

PEERDIR(
    contrib/libs/silk/src/fibers
    contrib/restricted/googletest/googletest
)

SRCS(
    silk_test_env.cpp
    blocking-queue-test.cpp
    cpu-test.cpp
    event-test.cpp
    fair-mutex-test.cpp
    fiber-test.cpp
    futex-test.cpp
    future-test.cpp
    mutex-test.cpp
    sequencer-test.cpp
    thread-mode-test.cpp
)

END()
