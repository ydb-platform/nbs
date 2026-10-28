UNITTEST_FOR(cloud/storage/core/libs/icv2/transport)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    transport_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/protos
)

END()
