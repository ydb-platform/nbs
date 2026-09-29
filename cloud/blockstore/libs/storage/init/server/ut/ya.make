UNITTEST_FOR(cloud/blockstore/libs/storage/init/server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    actorsystem_ut.cpp
)

PEERDIR(
    contrib/ydb/library/keys
)

YQL_LAST_ABI_VERSION()

END()
