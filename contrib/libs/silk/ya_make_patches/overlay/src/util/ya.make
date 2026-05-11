LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(${ARCADIA_ROOT}/contrib/libs/silk/LICENSE)

CXXFLAGS(-std=c++20)

ADDINCL(
    GLOBAL contrib/libs/silk/include
    GLOBAL contrib/libs/silk/src
    GLOBAL contrib/restricted/boost/intrusive/include
    contrib/libs/backtrace
)

PEERDIR(
    contrib/libs/rseq
    contrib/libs/backtrace
    contrib/restricted/boost/context/fcontext_impl
    contrib/restricted/boost/intrusive
)

CFLAGS(
    -DSILK_USE_LIBBACKTRACE
)

SRCS(
    assert.cpp
    init.cpp
    logger.cpp
    memory-pool.cpp
    perf.cpp
    queue.cpp
    sharded-stack.cpp
    tsc.cpp
)

END()

RECURSE_FOR_TESTS(
    tests
)
