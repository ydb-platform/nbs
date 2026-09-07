UNITTEST_FOR(cloud/storage/core/libs/journalled_device)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    journalled_device_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
)

END()
