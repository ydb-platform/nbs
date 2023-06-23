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

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()
