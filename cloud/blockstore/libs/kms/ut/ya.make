UNITTEST_FOR(cloud/blockstore/libs/kms)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

TAG(
    ya:not_autocheck
    ya:manual
)

SRCS(
    kms_client_ut.cpp
)

END()
