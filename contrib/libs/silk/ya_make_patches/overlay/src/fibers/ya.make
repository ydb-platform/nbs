LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(${ARCADIA_ROOT}/contrib/libs/silk/LICENSE)

CXXFLAGS(-std=c++20)

IF (SANITIZER_TYPE == undefined)
    CXXFLAGS(-fno-sanitize=function)
ENDIF()

PEERDIR(
    contrib/libs/silk/src/util
    contrib/libs/liburing
    contrib/restricted/boost/context/fcontext_impl
    contrib/restricted/boost/intrusive
)

SRCS(
    cpu.cpp
    fiber.cpp
    futex.cpp
    future.cpp
    histogram.cpp
    mutex.cpp
    profiler.cpp
    sequencer.cpp
)

END()

RECURSE_FOR_TESTS(
    tests
)
