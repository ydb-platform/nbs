PROGRAM(blockstore-dump-event-log)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/tools/analytics/libs/event-log

    contrib/libs/sqlite3
    library/cpp/eventlog/dumper
)

SRCS(
    sqlite_output.cpp
    main.cpp
)

END()
