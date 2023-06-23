UNITTEST_FOR(cloud/blockstore/libs/notify)

SRCS(
    notify_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/notify-mock/notify-mock.inc)

END()
