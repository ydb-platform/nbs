UNITTEST_FOR(cloud/blockstore/libs/icv2/transport)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    handshake_ut.cpp
    request_channel_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/tcp/testlib
    cloud/storage/core/libs/icv2/tcp
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/protos
)

END()
