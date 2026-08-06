UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../zero_ranges_stat.cpp
    zero_ranges_stat_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/tools/analytics/libs/event-log

    library/cpp/eventlog/dumper
)

END()
