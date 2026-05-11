GTEST()

LICENSE(Apache-2.0)

LICENSE_TEXTS(${ARCADIA_ROOT}/contrib/libs/silk/LICENSE)

NO_COMPILER_WARNINGS()

CXXFLAGS(-std=c++20)

PEERDIR(
    contrib/libs/silk/src/util
    contrib/restricted/googletest/googletest
)

SRCS(
    silk_test_env.cpp
    bounded-queue-test.cpp
    list-test.cpp
    memory-pool-test.cpp
    perf-test.cpp
    queue-test.cpp
    sharded-stack-test.cpp
    spinlock-test.cpp
    stack-test.cpp
    tree-test.cpp
    tsc-test.cpp
)

END()
