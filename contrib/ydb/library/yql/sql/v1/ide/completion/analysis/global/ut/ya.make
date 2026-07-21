UNITTEST_FOR(contrib/ydb/library/yql/sql/v1/ide/completion/analysis/global)

PEERDIR(
    contrib/ydb/library/yql/utils/string
)

SRCS(
    global_ut.cpp
    named_node_resolution_ut.cpp
)

END()
