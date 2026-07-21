UNITTEST_FOR(contrib/ydb/mvp/meta/support_links)

SIZE(SMALL)

SRCS(
    grafana_dashboard_source_ut.cpp
    grafana_dashboard_search_source_ut.cpp
    grafana_logging_source_ut.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/mvp/meta
    contrib/ydb/core/testlib/actors
)

END()
