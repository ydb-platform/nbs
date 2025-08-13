UNITTEST_FOR(cloud/filestore/libs/diagnostics)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    profile_log_events_ut.cpp
    profile_log_ut.cpp
    request_stats_ut.cpp
    user_counter_ut.cpp
)

PEERDIR(
    library/cpp/eventlog/dumper
)

RESOURCE(
    data/user_counters_empty.json user_counters_empty.json
    data/user_counters.json user_counters.json
)

END()
