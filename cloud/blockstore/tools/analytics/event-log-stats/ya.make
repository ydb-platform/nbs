PROGRAM(blockstore-event-log-stats)

PEERDIR(
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/blockstore/tools/analytics/libs/event-log

    library/cpp/eventlog/dumper
    library/cpp/getopt
    library/cpp/monlib/counters
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()

RECURSE_FOR_TESTS(
    tests
)
