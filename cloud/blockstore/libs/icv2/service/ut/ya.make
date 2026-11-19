UNITTEST_FOR(cloud/blockstore/libs/icv2/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    host_endpoint_ut.cpp
    ping_client_ut.cpp
    service_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/transport/testlib
    contrib/ydb/library/actors/dnsresolver
    contrib/ydb/library/actors/testlib
)

END()
