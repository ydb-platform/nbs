UNITTEST_FOR(contrib/ydb/core/pgproxy)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/core/protos
)

SRCS(
    pg_proxy_ut.cpp
)

END()
