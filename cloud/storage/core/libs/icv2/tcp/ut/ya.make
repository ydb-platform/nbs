UNITTEST_FOR(cloud/storage/core/libs/icv2/tcp)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    connector_ut.cpp
    listener_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/tcp/testlib
    contrib/ydb/library/actors/interconnect
)

END()
