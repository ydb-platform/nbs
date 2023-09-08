UNITTEST_FOR(cloud/blockstore/libs/notify)

OWNER(g:cloud-nbs)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    notify_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/notify-mock/notify-mock.inc)

END()
