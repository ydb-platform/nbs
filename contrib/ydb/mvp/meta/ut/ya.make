UNITTEST_FOR(contrib/ydb/mvp/meta)

SIZE(SMALL)

SRCS(
    meta_cache_ut.cpp
    meta_capabilities_ut.cpp
    meta_support_links_ut.cpp
    meta_ut.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/core/testlib/actors
)

DATA(
    arcadia/contrib/ydb/mvp/meta/examples
)

END()
