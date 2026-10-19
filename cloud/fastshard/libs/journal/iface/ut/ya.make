UNITTEST_FOR(cloud/fastshard/libs/journal/iface)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    device_ut.cpp
)

PEERDIR(
    cloud/fastshard/libs/journal/impl

    library/cpp/testing/unittest
    library/cpp/threading/future
)

END()
