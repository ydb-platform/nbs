LIBRARY()

WITHOUT_LICENSE_TEXTS()

NO_COMPILER_WARNINGS()

CXXFLAGS(-std=c++20)

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
    mutex.cpp
    sequencer.cpp
)

END()

RECURSE_FOR_TESTS(
    tests
)
