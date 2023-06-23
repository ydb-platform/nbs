PROGRAM(blockstore-dump-event-log)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/tools/analytics/libs/event-log

    library/cpp/eventlog/dumper
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()
