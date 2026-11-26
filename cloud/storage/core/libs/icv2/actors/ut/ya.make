UNITTEST_FOR(cloud/storage/core/libs/icv2/actors)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    address_resolver_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/dnsresolver
    contrib/ydb/library/actors/testlib
)

END()
