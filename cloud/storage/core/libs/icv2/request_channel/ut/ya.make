UNITTEST_FOR(cloud/storage/core/libs/icv2/request_channel)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    request_channel_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/icv2/handshake
    cloud/storage/core/libs/icv2/tcp/testlib
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/protos
)

END()
