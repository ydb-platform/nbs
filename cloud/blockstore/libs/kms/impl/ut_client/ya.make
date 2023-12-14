UNITTEST_FOR(cloud/blockstore/libs/kms/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TAG(
    ya:not_autocheck
    ya:manual
)

SRCS(
    kms_client_ut.cpp
)

END()
