UNITTEST_FOR(contrib/ydb/mvp/meta)

SIZE(SMALL)

SRCS(
    meta_cache_ut.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/core/testlib/actors
)

END()
