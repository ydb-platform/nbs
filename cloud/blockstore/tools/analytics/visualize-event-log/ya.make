PROGRAM(blockstore-visualize-event-log)

PEERDIR(
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service

    library/cpp/getopt
    library/cpp/eventlog/dumper
)

SRCS(
    main.cpp
)

END()
