GTEST()

LICENSE(Apache-2.0)

LICENSE_TEXTS(${ARCADIA_ROOT}/contrib/libs/silk/LICENSE)

NO_COMPILER_WARNINGS()

CXXFLAGS(-std=c++20)

ADDINCL(
    contrib/libs/silk/src
)

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
    fiber-thread-mode-test.cpp
)

# fiber-cpuset-test.cpp is CMake-only: it needs its own main that restricts
# the scheduler to a CPU subset before initialize, which the GTEST module
# cannot provide - gtest_main owns main and silk_test_env.cpp initializes
# the scheduler with default options.

END()
