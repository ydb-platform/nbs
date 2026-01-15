UNITTEST_FOR(cloud/blockstore/libs/notify/iface)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    notify_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/notify-mock/notify-mock.inc)

END()
