UNITTEST_FOR(contrib/ydb/core/pgproxy)

PEERDIR(
    library/cpp/actors/testlib
    contrib/ydb/core/protos
)

SRCS(
    pg_proxy_ut.cpp
)

END()
