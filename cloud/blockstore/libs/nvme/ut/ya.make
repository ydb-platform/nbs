UNITTEST_FOR(cloud/blockstore/libs/nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    device_locker_ut.cpp
)

END()
