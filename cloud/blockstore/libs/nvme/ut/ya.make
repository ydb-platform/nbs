UNITTEST_FOR(cloud/blockstore/libs/nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TAG(
    ya:not_autocheck
    ya:manual
)

SRCS(
    nvme_ut.cpp
)

END()
