UNITTEST_FOR(contrib/ydb/public/lib/ydb_cli/commands/topic_workload)

SRCS(
    topic_workload_params_ut.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    library/cpp/getopt/small
    contrib/ydb/public/lib/ydb_cli/commands/topic_workload  
)

END()
