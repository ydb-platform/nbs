UNITTEST_FOR(cloud/blockstore/libs/local_nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    service_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
