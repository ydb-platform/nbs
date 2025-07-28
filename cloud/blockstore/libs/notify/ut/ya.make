UNITTEST_FOR(cloud/blockstore/libs/notify)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    notify_ut.cpp
    json_generator_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/iam/iface
    library/cpp/testing/unittest
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/notify-mock/notify-mock.inc)

END()
