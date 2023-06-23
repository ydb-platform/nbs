PROGRAM(blockstore-event-log-disk-usage)

PEERDIR(
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/blockstore/tools/analytics/libs/event-log

    cloud/storage/core/libs/common

    library/cpp/getopt
    library/cpp/eventlog/dumper
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()
