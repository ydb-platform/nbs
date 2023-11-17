PROGRAM(blockstore-event-log-suffer)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/blockstore/tools/analytics/libs/event-log

    library/cpp/eventlog/dumper
    library/cpp/getopt
)

SRCS(
    main.cpp
)

END()
