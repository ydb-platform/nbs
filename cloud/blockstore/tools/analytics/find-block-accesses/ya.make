PROGRAM(blockstore-find-block-accesses)

PEERDIR(
    cloud/blockstore/libs/common
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
